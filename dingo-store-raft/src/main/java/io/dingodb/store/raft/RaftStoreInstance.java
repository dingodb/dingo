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
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.store.Part;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Files;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.PreParameters;
import io.dingodb.raft.core.DefaultJRaftServiceFactory;
import io.dingodb.raft.kv.storage.ByteArrayEntry;
import io.dingodb.raft.kv.storage.RawKVStore;
import io.dingodb.raft.kv.storage.RocksRawKVStore;
import io.dingodb.raft.kv.storage.SeekableIterator;
import io.dingodb.raft.option.RaftLogStoreOptions;
import io.dingodb.raft.storage.LogStore;
import io.dingodb.raft.storage.impl.RocksDBLogStore;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.raft.config.StoreConfiguration;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.dingodb.common.util.ByteArrayUtils.EMPTY_BYTES;
import static io.dingodb.common.util.ByteArrayUtils.compare;

@Slf4j
public class RaftStoreInstance implements StoreInstance {
    @Getter
    private final CommonId id;
    @Getter
    private final RawKVStore store;
    private final LogStore<RaftLogStoreOptions> logStore;
    private final Path path;
    private final Path dbPath;
    private final Path logPath;
    private final Map<CommonId, RaftStoreInstancePart> parts;
    private final NavigableMap<byte[], Part> startKeyPartMap;
    private final Map<byte[], RaftStoreInstancePart> waitParts;
    private final List<RaftStoreInstancePart> waitStoreParts;

    public RaftStoreInstance(Path path, CommonId id)  {
        try {
            this.id = id;
            this.path = path;
            Files.createDirectories(path);
            Files.createDirectories(dbPath = Paths.get(path.toString(), "db"));
            Files.createDirectories(logPath = Paths.get(path.toString(), "log"));
            this.store = new RocksRawKVStore(dbPath.toString(), StoreConfiguration.rocks());
            this.logStore = new RocksDBLogStore();
            RaftLogStoreOptions logStoreOptions = new RaftLogStoreOptions();
            logStoreOptions.setDataPath(logPath.toString());
            logStoreOptions.setLogEntryCodecFactory(DefaultJRaftServiceFactory
                .newInstance().createLogEntryCodecFactory());
            if (!this.logStore.init(logStoreOptions)) {
                log.error("Fail to init [RocksDBLogStore]");
                throw new RuntimeException("Fail to init [RocksDBLogStore]");
            }
            this.startKeyPartMap = new ConcurrentSkipListMap<>(ByteArrayUtils::compare);
            this.parts = new ConcurrentHashMap<>();
            this.waitParts = new ConcurrentSkipListMap<>(ByteArrayUtils::compare);
            this.waitStoreParts = new CopyOnWriteArrayList<>();
            log.info("Start raft store instance, id: {}", id);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void clear() {
        log.info("Clear store instance, id: [{}], data path: [{}]", id, path.toString());
        startKeyPartMap.clear();
        parts.values().forEach(RaftStoreInstancePart::clear);
        parts.clear();
        store.close();
        Files.deleteIfExists(path);
    }

    @Override
    public void assignPart(Part part) {
        part.setStart(PreParameters.cleanNull(part.getStart(), EMPTY_BYTES));
        try {
            Path partPath = Optional.ofNullable(StoreConfiguration.raft().getRaftPath())
                .filter(s -> !s.isEmpty())
                .ifAbsentSet(path::toString)
                .map(p -> Paths.get(p, part.getId().toString()))
                .ifPresent(Files::createDirectories)
                .get();
            RaftStoreInstancePart storeInstancePart = new RaftStoreInstancePart(part, partPath, store, logStore);
            storeInstancePart.getStateMachine().listenAvailable(() -> onPartAvailable(storeInstancePart));
            storeInstancePart.init();
            parts.put(part.getId(), storeInstancePart);
            waitStoreParts.add(storeInstancePart);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reassignPart(Part part) {
        RaftStoreInstancePart storeInstancePart = parts.get(part.getId());
        storeInstancePart.resetPart(part);
        storeInstancePart.getStateMachine().setEnable(false);
        if (startKeyPartMap.containsKey(part.getStart())) {
            storeInstancePart.getRaftStore().sync();
            startKeyPartMap.put(part.getStart(), part);
        }
        storeInstancePart.getStateMachine().setEnable(true);

    }

    @Override
    public void unassignPart(Part part) {
        part.setStart(PreParameters.cleanNull(part.getStart(), EMPTY_BYTES));
        parts.remove(part.getId()).clear();
        startKeyPartMap.remove(part.getStart());
    }

    @Override
    public void deletePart(Part part) {
        unassignPart(part);
        store.delete(part.getStart(), part.getEnd());
        log.info("Delete store instance part, id: [{}], part: {}", part.getId(), part);
    }

    public void onPartAvailable(RaftStoreInstancePart part) {
        log.info("The part {} available change {}", part.getId(), part.getStateMachine().isAvailable());
        if (part.getStateMachine().isAvailable()) {
            startKeyPartMap.put(part.getPart().getStart(), part.getPart());
        } else {
            startKeyPartMap.remove(part.getPart().getStart());
        }
    }

    public RaftStoreInstancePart getPart(CommonId id) {
        return parts.get(id);
    }

    public Part getPart(byte[] primaryKey) {
        return Optional.ofNullable(startKeyPartMap.floorEntry(primaryKey))
            .map(Map.Entry::getValue)
            .filter(part -> part.getEnd() == null || compare(primaryKey, part.getEnd()) < 0)
            .orNull();
    }

    @Override
    public boolean exist(byte[] primaryKey) {
        Part part;
        if ((part = getPart(primaryKey)) == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
            );
        }
        return parts.get(part.getId()).exist(primaryKey);
    }

    @Override
    public boolean existAny(List<byte[]> primaryKeys) {
        Part part = null;
        for (byte[] primaryKey : primaryKeys) {
            if (part == null && (part = getPart(primaryKey)) == null) {
                throw new IllegalArgumentException(
                    "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
                );
            }
            if (part != getPart(primaryKey)) {
                throw new IllegalArgumentException("The primary key list not in same part.");
            }
        }
        return parts.get(part.getId()).existAny(primaryKeys);
    }

    @Override
    public boolean existAny(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        Part part = getPart(startPrimaryKey);
        if (part == null || part != getPart(endPrimaryKey)) {
            throw new IllegalArgumentException("The start and end not in same part or not in current instance.");
        }
        return parts.get(part.getId()).existAny(startPrimaryKey, endPrimaryKey);
    }

    @Override
    public boolean upsertKeyValue(KeyValue row) {
        Part part = getPart(row.getPrimaryKey());
        if (part == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(row.getKey()) + " not in current instance."
            );
        }
        return parts.get(part.getId()).upsertKeyValue(row);
    }

    @Override
    public boolean upsertKeyValue(byte[] primaryKey, byte[] row) {
        Part part = getPart(primaryKey);
        if (part == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
            );
        }
        return parts.get(part.getId()).upsertKeyValue(primaryKey, row);
    }

    @Override
    public boolean upsertKeyValue(List<KeyValue> rows) {
        Part part = null;
        for (KeyValue row: rows) {

            if (part == null && (part = getPart(row.getPrimaryKey())) == null) {
                throw new IllegalArgumentException(
                    "The primary key " + Arrays.toString(row.getPrimaryKey()) + " not in current instance."
                );
            }
            if (part != getPart(row.getPrimaryKey())) {
                throw new IllegalArgumentException("The primary key list not in same part.");
            }
        }
        return parts.get(part.getId()).upsertKeyValue(rows);
    }

    @Override
    public boolean delete(byte[] primaryKey) {
        Part part = getPart(primaryKey);
        if (part == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
            );
        }
        return parts.get(part.getId()).delete(primaryKey);
    }

    @Override
    public boolean delete(List<byte[]> primaryKeys) {
        Part part = null;
        for (byte[] primaryKey : primaryKeys) {
            if (part == null && (part = getPart(primaryKey)) == null) {
                throw new IllegalArgumentException(
                    "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
                );
            }
            if (part != getPart(primaryKey)) {
                throw new IllegalArgumentException("The primary key list not in same part.");
            }
        }
        return parts.get(part.getId()).delete(primaryKeys);
    }

    @Override
    public boolean delete(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        Part part = getPart(startPrimaryKey);
        if (part == null || part != getPart(endPrimaryKey)) {
            throw new IllegalArgumentException("The start and end not in same part or not in current instance.");
        }
        return parts.get(part.getId()).delete(startPrimaryKey, endPrimaryKey);
    }

    @Override
    public byte[] getValueByPrimaryKey(byte[] primaryKey) {
        Part part = getPart(primaryKey);
        if (part == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
            );
        }
        return parts.get(part.getId()).getValueByPrimaryKey(primaryKey);
    }

    @Override
    public List<KeyValue> getKeyValueByPrimaryKeys(List<byte[]> primaryKeys) {
        Part part = null;
        for (byte[] primaryKey : primaryKeys) {
            if (part == null && (part = getPart(primaryKey)) == null) {
                throw new IllegalArgumentException(
                    "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
                );
            }
            if (part != getPart(primaryKey)) {
                throw new IllegalArgumentException("The primary key list not in same part.");
            }
        }
        return parts.get(part.getId()).getKeyValueByPrimaryKeys(primaryKeys);
    }

    @Override
    public Iterator<KeyValue> keyValueScan() {
        return new FullScanRawIterator(parts.values().stream().map(RaftStoreInstancePart::iterator).iterator());
    }

    @Override
    public Iterator<KeyValue> keyValueScan(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        Part part = getPart(startPrimaryKey);
        if (part == null || part != getPart(endPrimaryKey)) {
            throw new IllegalArgumentException("The start and end not in same part or not in current instance.");
        }
        return parts.get(part.getId()).keyValueScan(startPrimaryKey, endPrimaryKey);
    }

    class FullScanRawIterator extends KeyValueIterator {
        private Iterator<SeekableIterator<byte[], ByteArrayEntry>> partIterator;

        public FullScanRawIterator(Iterator<SeekableIterator<byte[], ByteArrayEntry>> partIterator) {
            super(partIterator.next());
            this.partIterator = partIterator;
        }

        @Override
        public boolean hasNext() {
            while (!iterator.hasNext()) {
                if (!partIterator.hasNext()) {
                    return false;
                }
                iterator = partIterator.next();
            }
            return true;
        }
    }

}
