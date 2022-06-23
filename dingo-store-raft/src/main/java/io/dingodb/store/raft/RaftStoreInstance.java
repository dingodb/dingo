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
import io.dingodb.server.protocol.metric.MonitorMetric;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.raft.config.StoreConfiguration;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

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
    private final PartReadWriteCollector collector;

    public RaftStoreInstance(Path path, CommonId id) {
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
            this.collector = PartReadWriteCollector.instance();
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
        waitParts.clear();
        store.close();
        logStore.shutdown();
        Files.deleteIfExists(path);
    }

    @Override
    public void closeReportStats(CommonId part) {
        parts.get(part).getStateMachine().collectStats(false);
    }

    @Override
    public void openReportStats(CommonId part) {
        parts.get(part).getStateMachine().collectStats(true);
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reassignPart(Part part) {
        // todo how to ensure consistency when reassign?
        RaftStoreInstancePart storeInstancePart = parts.get(part.getId());
        storeInstancePart.resetPart(part);
        startKeyPartMap.computeIfPresent(part.getStart(), (key, old) -> {
            storeInstancePart.getRaftStore().sync();
            return part;
        });
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

    @Override
    public long countOrDeletePart(byte[] startKey, boolean doDeleting) {
        Part part = getPart(startKey);
        if (part == null) {
            log.warn("delete store by part start key. but find start key:{} not in any part",
                Arrays.toString(startKey));
            return 0;
        }
        return parts.get(part.getId()).countOrDeletePart(startKey, doDeleting);
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
        long startTime = System.currentTimeMillis();
        Part part = getPart(primaryKey);
        if (part == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
            );
        }
        boolean exist = parts.get(part.getId()).exist(primaryKey);
        this.collector.putSample(System.currentTimeMillis() - startTime, part.getId(), MonitorMetric.PART_READ);
        return exist;
    }

    @Override
    public boolean existAny(List<byte[]> primaryKeys) {
        Part part = null;
        long startTime = System.currentTimeMillis();
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
        boolean exist = parts.get(part.getId()).existAny(primaryKeys);
        this.collector.putSample(System.currentTimeMillis() - startTime, part.getId(), MonitorMetric.PART_READ);
        return exist;
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
        long startTime = System.currentTimeMillis();
        Part part = getPart(row.getPrimaryKey());
        if (part == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(row.getKey()) + " not in current instance."
            );
        }
        boolean result = parts.get(part.getId()).upsertKeyValue(row);
        this.collector.putSample(System.currentTimeMillis() - startTime, part.getId(), MonitorMetric.PART_WRITE);
        return result;
    }

    @Override
    public boolean upsertKeyValue(byte[] primaryKey, byte[] row) {
        long startTime = System.currentTimeMillis();
        Part part = getPart(primaryKey);
        if (part == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
            );
        }
        boolean result = parts.get(part.getId()).upsertKeyValue(primaryKey, row);
        this.collector.putSample(System.currentTimeMillis() - startTime, part.getId(), MonitorMetric.PART_WRITE);
        return result;
    }

    @Override
    public boolean upsertKeyValue(List<KeyValue> rows) {
        Part part = null;
        long startTime = System.currentTimeMillis();
        for (KeyValue row : rows) {
            if (part == null && (part = getPart(row.getPrimaryKey())) == null) {
                throw new IllegalArgumentException(
                    "The primary key " + Arrays.toString(row.getPrimaryKey()) + " not in current instance."
                );
            }
            if (part != getPart(row.getPrimaryKey())) {
                throw new IllegalArgumentException("The primary key list not in same part.");
            }
        }
        boolean result = parts.get(part.getId()).upsertKeyValue(rows);
        this.collector.putSample(System.currentTimeMillis() - startTime, part.getId(), MonitorMetric.PART_WRITE);
        return result;
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
        boolean isSuccess = false;
        try {
            Map<Part, List<byte[]>> keysGroupByPart = groupKeysByPart(primaryKeys);
            for (Map.Entry<Part, List<byte[]>> entry : keysGroupByPart.entrySet()) {
                Part part = entry.getKey();
                Optional<List<byte[]>> keysInPart = Optional.of(entry.getValue());
                if (keysInPart.isPresent()) {
                    isSuccess = parts.get(part.getId()).delete(keysInPart.get());
                    if (!isSuccess) {
                        log.error("Delete failed, part: {}, keysCnt: {}", part.getId(), keysInPart.get().size());
                    }
                } else {
                    log.warn("Delete failed, part: {}, keysCnt: 0", part.getId());
                }
            }
            return isSuccess;
        } catch (IllegalArgumentException e) {
            log.error("Delete Id:{} by keys failed: {}", this.id, e.getMessage());
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    public boolean delete(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        boolean isSuccess = true;
        try {
            Map<Part, List<byte[]>> mappingByPartToKeys = groupKeysByPart(startPrimaryKey, endPrimaryKey);

            for (Map.Entry<Part, List<byte[]>> entry : mappingByPartToKeys.entrySet()) {
                Part part = entry.getKey();
                List<byte[]> keys = entry.getValue();
                boolean isOK = parts.get(part.getId()).delete(keys.get(0), keys.get(1));
                if (!isOK) {
                    isSuccess = false;
                    log.warn("Delete partition failed: part: " + part.getId() + ", keys: " + keys);
                }
            }
        } catch (IllegalArgumentException e) {
            log.error("Delete failed, startPrimaryKey: {}, endPrimaryKey: {}", startPrimaryKey, endPrimaryKey, e);
        }
        return isSuccess;
    }

    private Map<Part, List<byte[]>> groupKeysByPart(List<byte[]> primaryKeys) {
        Map<Part, List<byte[]>> result = new HashMap<>();
        for (byte[] primaryKey : primaryKeys) {
            Part part = getPart(primaryKey);
            if (part == null) {
                throw new IllegalArgumentException(
                    "The primary key " + Arrays.toString(primaryKey) + " can not compute part info."
                );
            }
            List<byte[]> list = result.get(part);
            if (list == null) {
                list = new ArrayList<>();
                result.put(part, list);
            }
            list.add(primaryKey);
        }
        return result;
    }

    public Map<Part, List<byte[]>> groupKeysByPart(byte[] startKey, byte[] endKey) {
        Part startPart = getPart(startKey);
        Part endPart = getPart(endKey);

        // case1. startKey and endKey is in same part, then reture the part
        if (startPart == endPart) {
            return Collections.singletonMap(startPart, Arrays.asList(startKey, endKey));
        }

        // case2. compute the partition list by <startKey, endKey>
        List<byte[]> keyArraysList = startKeyPartMap.keySet().stream().collect(Collectors.toList());
        if (keyArraysList.size() <= 1) {
            throw new IllegalArgumentException("Invalid Key Partition Map, should more than 1.");
        }

        int startIndex = 0;
        int endIndex = keyArraysList.size() - 1;

        for (int i = 0; i < keyArraysList.size(); i++) {
            byte[] keyInList = keyArraysList.get(i);
            if (ByteArrayUtils.compare(startKey, keyInList) <= 0) {
                startIndex = i - 1;
                break;
            }
        }

        for (int i = keyArraysList.size() - 1; i >= 0; i--) {
            byte[] keyInList = keyArraysList.get(i);
            if (ByteArrayUtils.compare(endKey, keyInList) >= 0) {
                endIndex = i;
                break;
            }
        }

        if (startIndex < 0 || endIndex < 0 || startIndex >= endIndex) {
            log.warn("Invalid Key Partition Map, startIndex: {}, endIndex: {}", startIndex, endIndex);
            throw new IllegalArgumentException("Invalid Key Partition Map, startIndex: "
                + startIndex + ", endIndex: " + endIndex);
        }

        Map<Part, List<byte[]>> result = new HashMap<>();
        for (int i = startIndex; i <= endIndex; i++) {
            if (i == startIndex) {
                // first partition
                byte[] keyInList = keyArraysList.get(i + 1);
                Part part = getPart(startKey);
                List<byte[]> list = Arrays.asList(startKey, keyInList);
                result.put(part, list);
            } else if (i == endIndex) {
                // last partition
                byte[] keyInList = keyArraysList.get(i);
                Part part = getPart(endKey);
                List<byte[]> list = Arrays.asList(keyInList, endKey);
                result.put(part, list);
            } else {
                // middle partition
                byte[] keyInList = keyArraysList.get(i);
                byte[] keyInListNext = keyArraysList.get(i + 1);
                Part part = getPart(keyInList);
                List<byte[]> list = Arrays.asList(keyInList, keyInListNext);
                result.put(part, list);
            }
        }
        return result;
    }

    @Override
    public byte[] getValueByPrimaryKey(byte[] primaryKey) {
        long startTime = System.currentTimeMillis();
        Part part = getPart(primaryKey);
        if (part == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
            );
        }
        byte[] result = parts.get(part.getId()).getValueByPrimaryKey(primaryKey);
        this.collector.putSample(System.currentTimeMillis() - startTime, part.getId(), MonitorMetric.PART_READ);
        return result;
    }

    @Override
    public List<KeyValue> getKeyValueByPrimaryKeys(List<byte[]> primaryKeys) {
        long startTime = System.currentTimeMillis();
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
        List<KeyValue> result = parts.get(part.getId()).getKeyValueByPrimaryKeys(primaryKeys);
        this.collector.putSample(System.currentTimeMillis() - startTime, part.getId(), MonitorMetric.PART_READ);
        return result;
    }

    @Override
    public Iterator<KeyValue> keyValueScan() {
        if (startKeyPartMap.isEmpty()) {
            return new KeyValueIterator(Collections.emptyIterator());
        }
        return new FullScanRawIterator(startKeyPartMap.values().stream()
            .map(Part::getId)
            .map(parts::get)
            .map(RaftStoreInstancePart::iterator)
            .iterator());
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
        private final Iterator<SeekableIterator<byte[], ByteArrayEntry>> partIterator;

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
