/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.store.raft;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.store.Part;
import io.dingodb.raft.kv.storage.ByteArrayEntry;
import io.dingodb.raft.kv.storage.RaftRawKVStore;
import io.dingodb.raft.kv.storage.RawKVStore;
import io.dingodb.raft.kv.storage.SeekableIterator;
import io.dingodb.raft.storage.LogStore;
import io.dingodb.raft.storage.impl.RocksDBLogStorage;
import io.dingodb.raft.storage.impl.RocksDBLogStore;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.raft.config.StoreConfiguration;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Getter
public final class RaftStoreInstancePart implements StoreInstance {

    private CommonId id;
    private Part part;
    private RawKVStore store;
    private RaftRawKVStore raftStore;
    private PartStateMachine stateMachine;

    public RaftStoreInstancePart(Part part, Path path, RawKVStore store, LogStore logStore) throws Exception {
        this.id = part.getId();
        this.store = store;
        this.part = part;
        this.raftStore = new RaftRawKVStore(
            id,
            store,
            StoreConfiguration.raft().getNode(),
            path,
            new RocksDBLogStorage(id.seqContent(), (RocksDBLogStore) logStore),
            new Location(DingoConfiguration.host(), StoreConfiguration.raft().getPort()),
            part.getReplicates()
        );
        this.stateMachine = new PartStateMachine(id, raftStore, part);
        raftStore.getNodeOptions().setFsm(stateMachine);
        log.info("Start raft store instance part, id: {}, part: {}", id, part);
    }

    public void resetPart(Part part) {
        this.part = part;
        this.stateMachine.resetPart(part);
    }

    public void init() {
        this.raftStore.init(null);
    }

    public void clear() {
        log.info("Clear raft store instance part, id: {}", id.toString());
        raftStore.shutdown();
        log.info("Raft store closed, id: {}", id.toString());
    }

    public SeekableIterator<byte[], ByteArrayEntry> iterator() {
        if (!stateMachine.isEnable()) {
            throw new UnsupportedOperationException("State machine not available");
        }
        return raftStore.scan(part.getStart(), part.getEnd()).join();
    }

    @Override
    public boolean exist(byte[] primaryKey) {
        if (!stateMachine.isEnable()) {
            throw new UnsupportedOperationException("State machine not available");
        }
        return raftStore.containsKey(primaryKey).join();
    }

    @Override
    public boolean existAny(List<byte[]> primaryKeys) {
        if (!stateMachine.isEnable()) {
            throw new UnsupportedOperationException("State machine not available");
        }
        for (byte[] primaryKey : primaryKeys) {
            if (exist(primaryKey)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean existAny(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        if (!stateMachine.isEnable()) {
            throw new UnsupportedOperationException("State machine not available");
        }
        return keyValueScan(startPrimaryKey, endPrimaryKey).hasNext();
    }

    @Override
    public boolean upsertKeyValue(KeyValue row) {
        if (!stateMachine.isEnable()) {
            throw new UnsupportedOperationException("State machine not available");
        }
        return raftStore.put(row.getPrimaryKey(), row.getValue()).join();
    }

    @Override
    public boolean upsertKeyValue(byte[] primaryKey, byte[] row) {
        if (!stateMachine.isEnable()) {
            throw new UnsupportedOperationException("State machine not available");
        }
        return raftStore.put(primaryKey, row).join();
    }

    @Override
    public boolean upsertKeyValue(List<KeyValue> rows) {
        if (!stateMachine.isEnable()) {
            throw new UnsupportedOperationException("State machine not available");
        }
        return raftStore.put(rows.stream()
            .filter(Objects::nonNull)
            .map(row -> new ByteArrayEntry(row.getPrimaryKey(), row.getValue()))
            .collect(Collectors.toList())
        ).join();
    }

    @Override
    public byte[] getValueByPrimaryKey(byte[] primaryKey) {
        if (!stateMachine.isEnable()) {
            throw new UnsupportedOperationException("State machine not available");
        }
        return raftStore.get(primaryKey).join();
    }

    @Override
    public List<KeyValue> getKeyValueByPrimaryKeys(List<byte[]> primaryKeys) {
        if (!stateMachine.isEnable()) {
            throw new UnsupportedOperationException("State machine not available");
        }
        return raftStore.get(primaryKeys).join().stream()
            .filter(Objects::nonNull)
            .map(e -> new KeyValue(e.getKey(), e.getValue())).collect(Collectors.toList());
    }

    @Override
    public Iterator<KeyValue> keyValueScan() {
        if (!stateMachine.isEnable()) {
            throw new UnsupportedOperationException("State machine not available");
        }
        return new KeyValueIterator(iterator());
    }

    @Override
    public Iterator<KeyValue> keyValueScan(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        if (!stateMachine.isEnable()) {
            throw new UnsupportedOperationException("State machine not available");
        }
        return new KeyValueIterator(raftStore.scan(startPrimaryKey, endPrimaryKey).join());
    }

    @Override
    public boolean delete(byte[] key) {
        if (!stateMachine.isEnable()) {
            throw new UnsupportedOperationException("State machine not available");
        }
        return raftStore.delete(key).join();
    }

    @Override
    public boolean delete(List<byte[]> primaryKeys) {
        if (!stateMachine.isEnable()) {
            throw new UnsupportedOperationException("State machine not available");
        }
        return raftStore.delete(primaryKeys).join();
    }

    @Override
    public boolean delete(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        if (!stateMachine.isEnable()) {
            throw new UnsupportedOperationException("State machine not available");
        }
        return raftStore.delete(startPrimaryKey, endPrimaryKey).join();
    }

}
