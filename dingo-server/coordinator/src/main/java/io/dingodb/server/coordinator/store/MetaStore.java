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

package io.dingodb.server.coordinator.store;

import io.dingodb.common.CommonId;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.Optional;
import io.dingodb.raft.Node;
import io.dingodb.raft.kv.storage.ByteArrayEntry;
import io.dingodb.raft.kv.storage.RaftRawKVOperation;
import io.dingodb.raft.kv.storage.RawKVStore;
import io.dingodb.raft.kv.storage.ReadIndexRunner;
import io.dingodb.raft.kv.storage.SeekableIterator;
import io.dingodb.server.protocol.CommonIdConstant;
import io.dingodb.store.api.StoreInstance;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class MetaStore implements StoreInstance {
    private static final byte[] SEQ_PREFIX = CommonId.prefix(
        CommonIdConstant.ID_TYPE.data,
        CommonIdConstant.DATA_IDENTIFIER.seq
    ).content();

    private final Node node;
    private final ReadIndexRunner readIndexRunner;
    private final RawKVStore store;

    public MetaStore(Node node, RawKVStore store) {
        this.node = node;
        this.store = store;
        this.readIndexRunner = new ReadIndexRunner(node, this::readInStore);
    }

    public synchronized int generateSeq(byte[] key) {
        byte[] realKey = new byte[key.length + SEQ_PREFIX.length];
        System.arraycopy(SEQ_PREFIX, 0, realKey, 0, SEQ_PREFIX.length);
        System.arraycopy(key, 0, realKey, SEQ_PREFIX.length, key.length);
        return Optional.ofNullable(PrimitiveCodec.readVarInt(getValueByPrimaryKey(realKey)))
            .ifAbsentSet(0)
            .map(seq -> seq + 1)
            .ifPresent(seq -> upsertKeyValue(realKey, PrimitiveCodec.encodeVarInt(seq)))
            .get();
    }

    private <T> T write(RaftRawKVOperation operation) {
        return (T) operation.applyOnNode(this.node).join();
    }

    private <T> T read(RaftRawKVOperation operation) {
        return (T) readIndexRunner.readIndex(operation).join();
    }

    private Object readInStore(RaftRawKVOperation operation) {
        switch (operation.getOp()) {
            case GET:
                return store.get(operation.getKey());
            case MULTI_GET:
                return store.get((List<byte[]>) operation.getExt1());
            case ITERATOR:
                return store.iterator();
            case SCAN:
                return store.scan(operation.getKey(), operation.getExtKey());
            case CONTAINS_KEY:
                return store.containsKey(operation.getKey());
            default:
                throw new IllegalStateException("Not read operation: " + operation.getOp());
        }
    }

    public SeekableIterator<byte[], ByteArrayEntry> iterator() {
        return read(RaftRawKVOperation.iterator());
    }

    @Override
    public boolean exist(byte[] primaryKey) {
        return read(RaftRawKVOperation.containsKey(primaryKey));
    }

    @Override
    public boolean existAny(List<byte[]> primaryKeys) {
        for (byte[] primaryKey : primaryKeys) {
            if (read(RaftRawKVOperation.containsKey(primaryKey))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean existAny(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        return keyValueScan(startPrimaryKey, endPrimaryKey).hasNext();
    }

    @Override
    public byte[] getValueByPrimaryKey(byte[] primaryKey) {
        return read(RaftRawKVOperation.get(primaryKey));
    }

    @Override
    public List<KeyValue> getKeyValueByPrimaryKeys(List<byte[]> primaryKeys) {
        return ((List<KeyValue>) read(RaftRawKVOperation.get(primaryKeys))).stream()
            .filter(Objects::nonNull)
            .map(e -> new KeyValue(e.getKey(), e.getValue())).collect(Collectors.toList());
    }

    @Override
    public Iterator<KeyValue> keyValueScan() {
        return new KeyValueIterator(iterator());
    }

    @Override
    public Iterator<KeyValue> keyValueScan(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        return new KeyValueIterator(read(RaftRawKVOperation.scan(startPrimaryKey, endPrimaryKey)));
    }

    @Override
    public boolean upsertKeyValue(KeyValue row) {
        return write(RaftRawKVOperation.put(row.getKey(), row.getValue()));
    }

    @Override
    public boolean upsertKeyValue(byte[] primaryKey, byte[] row) {
        return write(RaftRawKVOperation.put(primaryKey, row));
    }

    @Override
    public boolean upsertKeyValue(List<KeyValue> rows) {
        return write(RaftRawKVOperation.put(rows.stream()
            .filter(Objects::nonNull)
            .map(row -> new ByteArrayEntry(row.getPrimaryKey(), row.getValue()))
            .collect(Collectors.toList())));
    }

    @Override
    public boolean delete(byte[] key) {
        return write(RaftRawKVOperation.delete(key));
    }

    @Override
    public boolean delete(List<byte[]> primaryKeys) {
        return write(RaftRawKVOperation.delete(primaryKeys));
    }

    @Override
    public boolean delete(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        return write(RaftRawKVOperation.delete(startPrimaryKey, endPrimaryKey));
    }

}
