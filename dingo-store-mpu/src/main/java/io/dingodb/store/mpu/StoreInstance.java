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

package io.dingodb.store.mpu;


import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.store.Part;
import io.dingodb.common.table.DingoKeyValueCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Parameters;
import io.dingodb.common.util.UdfUtils;
import io.dingodb.mpu.core.Core;
import io.dingodb.mpu.core.CoreListener;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.core.MirrorProcessingUnit;
import io.dingodb.mpu.instruction.KVInstructions;
import io.dingodb.mpu.storage.rocks.RocksUtils;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.api.ReportApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.protocol.meta.TablePartStats;
import io.dingodb.server.protocol.meta.TablePartStats.ApproximateStats;
import io.dingodb.store.mpu.instruction.OpInstructions;
import lombok.extern.slf4j.Slf4j;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dingodb.common.util.ByteArrayUtils.lessThan;
import static io.dingodb.common.util.ByteArrayUtils.lessThanOrEqual;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.STATS_IDENTIFIER;


@Slf4j
public class StoreInstance implements io.dingodb.store.api.StoreInstance {

    private final CommonId id;
    public final MirrorProcessingUnit mpu;
    public final Path path;
    private int ttl;
    private Map<String, Globals> globalsMap = new HashMap<>();
    private Map<String, TableDefinition> definitionMap = new HashMap<>();
    private Map<String, KeyValueCodec> codecMap = new HashMap<>();

    public StoreInstance(CommonId id, Path path) {
        this(id, path, "", "", -1);
    }

    private final Map<CommonId, Core> parts;
    private final NavigableMap<byte[], Part> startKeyPartMap;

    public StoreInstance(CommonId id, Path path, final String dbRocksOptionsFile, final String logRocksOptionsFile,
                         final int ttl) {
        this.id = id;
        this.path = path.toAbsolutePath();
        this.ttl = ttl;
        this.mpu = new MirrorProcessingUnit(id, this.path, dbRocksOptionsFile, logRocksOptionsFile, this.ttl);
        this.parts = new ConcurrentHashMap<>();
        this.startKeyPartMap = new ConcurrentSkipListMap<>(ByteArrayUtils::compare);
    }

    @Override
    public synchronized void assignPart(Part part) {
        Core coreAssign;
        List<CoreMeta> coreMetas = new ArrayList<>();
        List<Location> replicateLocations = part.getReplicateLocations();
        List<CommonId> replicates = part.getReplicates();
        for (int i = 0; i < replicates.size(); i++) {
            coreMetas.add(i, new CoreMeta(replicates.get(i), part.getId(), mpu.id,
                replicateLocations.get(i), 3 - i));
        }
        coreAssign = mpu.createCore(replicates.indexOf(part.getReplicateId()), coreMetas);
        coreAssign.registerListener(CoreListener.primary(__ -> sendStats(coreAssign)));
        coreAssign.registerListener(CoreListener.primary(__ -> onPartAvailable(coreAssign, part)));
        coreAssign.start();
        this.parts.put(part.getId(), coreAssign);
    }

    public void destroy() {
        startKeyPartMap.clear();
        parts.values().forEach(Core::destroy);
        parts.clear();
        /**
         * to avoid file handle leak, when drop table
         */
        // FileUtils.deleteIfExists(path);
    }

    public ApproximateStats approximateStats(Core core) {
        return new ApproximateStats(ByteArrayUtils.EMPTY_BYTES, null, approximateCount(core),
            approximateSize(core));
    }

    public long approximateCount(Core core) {
        return core.storage.approximateCount();
    }

    public long approximateSize(Core core) {
        return core.storage.approximateSize();
    }

    public void sendStats(Core core) {
        try {
            if (!core.isAvailable() && core.isPrimary()) {
                return;
            }
            CommonId partId = core.meta.coreId;
            TablePartStats stats = TablePartStats.builder()
                .id(new CommonId(ID_TYPE.stats, STATS_IDENTIFIER.part, partId.domainContent(), partId.seqContent()))
                .leader(DingoConfiguration.instance().getServerId())
                .tablePart(partId)
                .table(core.meta.mpuId)
                .approximateStats(Collections.singletonList(approximateStats(core)))
                .time(System.currentTimeMillis())
                .build();
            ApiRegistry.getDefault().proxy(ReportApi.class, CoordinatorConnector.defaultConnector())
                .report(stats);
        } catch (Exception e) {
            log.error("{} send stats to failed.", core.meta, e);
        }

        // todo
        // if (!core.isAvailable() && core.isPrimary()) {
        //     Executors.scheduleAsync(core.meta.label + "-send-stats", () -> sendStats(core), 5, TimeUnit.SECONDS);
        // }
    }

    public void onPartAvailable(Core core, Part part) {
        log.info("onPartAvailable :" + core);
        try {
            if (core.isAvailable() && core.isPrimary()) {
                startKeyPartMap.put(part.getStart(), part);
            } else {
                startKeyPartMap.remove(part.getStart());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public boolean exist(byte[] primaryKey) {
        Part part = getPartByPrimaryKey(primaryKey);
        if (part == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
            );
        }
        return parts.get(part.getId()).view(KVInstructions.id, KVInstructions.GET_OC, primaryKey) != null;
    }

    @Override
    public boolean existAny(List<byte[]> primaryKeys) {
        for (byte[] primaryKey : primaryKeys) {
            if (exist(primaryKey)) {
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
    public boolean upsertKeyValue(byte[] primaryKey, byte[] row) {
        Part part = getPartByPrimaryKey(primaryKey);
        if (part == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
            );
        }
        if (RocksUtils.ttlValid(this.ttl)) {
            parts.get(part.getId()).exec(KVInstructions.id, KVInstructions.SET_OC, primaryKey,
                RocksUtils.getValueWithNowTs(row)).join();
        } else {
            parts.get(part.getId()).exec(KVInstructions.id, KVInstructions.SET_OC, primaryKey, row).join();
        }

        return true;
    }

    @Override
    public boolean upsertKeyValue(KeyValue row) {
        return upsertKeyValue(row.getPrimaryKey(), row.getValue());
    }

    @Override
    public boolean upsertKeyValue(List<KeyValue> rows) {
        List<KeyValue> kvList;
        if (RocksUtils.ttlValid(this.ttl)) {
            kvList = RocksUtils.getValueWithNowTsList(rows);
        } else {
            kvList = rows;
        }

        List<byte[]> rowKeyList = kvList.stream().map(KeyValue::getPrimaryKey).collect(Collectors.toList());
        boolean isAllKeyOnSamePart = isKeysOnSamePart(rowKeyList);
        if (!isAllKeyOnSamePart) {
            log.warn("The input keys are not in a same instance.");
            throw new IllegalArgumentException("The key not in current instance.");
        }

        Part part = getPartByPrimaryKey(kvList.get(0).getPrimaryKey());
        parts.get(part.getId()).exec(
            KVInstructions.id, KVInstructions.SET_BATCH_OC,
            kvList.stream().flatMap(kv -> Stream.of(kv.getPrimaryKey(), kv.getValue())).toArray()
        ).join();
        return true;
    }

    @Override
    public byte[] getValueByPrimaryKey(byte[] primaryKey) {
        Part part = getPartByPrimaryKey(primaryKey);
        if (part == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
            );
        }
        return parts.get(part.getId()).view(KVInstructions.id, KVInstructions.GET_OC, primaryKey);
    }

    @Override
    public void deletePart(Part part) {
        // 1 clean parts
        // 2 clean startKeyPartMap
        part.setStart(Parameters.cleanNull(part.getStart(), ByteArrayUtils.EMPTY_BYTES));
        startKeyPartMap.remove(part.getStart());
        parts.get(part.getId()).exec(KVInstructions.id, KVInstructions.DEL_RANGE_OC,
            part.getStart(), part.getEnd()).join();
        parts.remove(part.getId());
    }

    @Override
    public long countOrDeletePart(byte[] startKey, boolean doDeleting) {
        Part part = getPartByPrimaryKey(startKey);
        if (part == null) {
            log.warn("Count or delete store by part start key. but find start key:{} not in any part",
                Arrays.toString(startKey));
            return 0;
        }
        CompletableFuture<Long> count = CompletableFuture.supplyAsync(
            () -> parts.get(part.getId()).view(KVInstructions.id, KVInstructions.COUNT_OC)
        );
        if (doDeleting) {
            parts.get(part.getId())
                .exec(
                KVInstructions.id,
                KVInstructions.DEL_RANGE_OC,
                   ByteArrayUtils.EMPTY_BYTES,
                null)
                .join();
        }
        return count.join();
    }

    @Override
    public long countDeleteByRange(byte[] startKey, byte[] endKey) {
        List<byte[]> keysInBytes = Arrays.asList(startKey, endKey);
        boolean isKeysOnSamePart = isKeysOnSamePart(keysInBytes);
        if (!isKeysOnSamePart) {
            throw new IllegalArgumentException("The start and end not in same part or not in current instance.");
        }

        Part part = getPartByPrimaryKey(startKey);
        CompletableFuture<Long> count = CompletableFuture.supplyAsync(
            () -> parts.get(part.getId()).view(KVInstructions.id, KVInstructions.COUNT_OC)
        );
        parts.get(part.getId()).exec(KVInstructions.id, KVInstructions.DEL_RANGE_OC,
            ByteArrayUtils.EMPTY_BYTES, null).join();
        return count.join();
    }

    @Override
    public KeyValue getKeyValueByPrimaryKey(byte[] primaryKey) {
        Part part = getPartByPrimaryKey(primaryKey);
        if (part == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
            );
        }
        return parts.get(part.getId()).view(KVInstructions.id, KVInstructions.GET_OC, primaryKey);
    }

    @Override
    public List<KeyValue> getKeyValueByPrimaryKeys(List<byte[]> primaryKeys) {
        boolean isKeysOnSamePart = isKeysOnSamePart(primaryKeys);
        if (!isKeysOnSamePart) {
            throw new IllegalArgumentException("The primary key list not in same part.");
        }
        Part part = getPartByPrimaryKey(primaryKeys.get(0));
        return parts.get(part.getId()).view(KVInstructions.id, KVInstructions.GET_BATCH_OC, primaryKeys);
    }

    @Override
    public Iterator<KeyValue> keyValueScan() {
        List<Iterator<KeyValue>> iterators = new ArrayList<>();
        startKeyPartMap.values().stream()
            .map(Part::getId)
            .map(parts::get)
            .forEach(store -> {
                iterators.add(store.view(KVInstructions.id, KVInstructions.SCAN_OC));
            });
        return new FullScanRawIterator(iterators.iterator());
    }

    @Override
    public Iterator<KeyValue> keyValueScan(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        isValidRangeKey(startPrimaryKey, endPrimaryKey);

        List<byte[]> keysInBytes = Arrays.asList(startPrimaryKey, endPrimaryKey);
        boolean isKeysOnSamePart = isKeysOnSamePart(keysInBytes);
        if (!isKeysOnSamePart) {
            throw new IllegalArgumentException("The start and end not in same part or not in a same instance.");
        }

        Part part = getPartByPrimaryKey(startPrimaryKey);
        return parts.get(part.getId()).view(KVInstructions.id, KVInstructions.SCAN_OC, startPrimaryKey, endPrimaryKey);
    }

    @Override
    public Iterator<KeyValue> keyValueScan(
        byte[] startPrimaryKey,
        byte[] endPrimaryKey,
        boolean includeStart,
        boolean includeEnd) {
        isValidRangeKey(startPrimaryKey, endPrimaryKey);
        List<byte[]> keysInBytes = Arrays.asList(startPrimaryKey, endPrimaryKey);
        boolean isKeysOnSamePart = isKeysOnSamePart(keysInBytes);
        if (!isKeysOnSamePart) {
            throw new IllegalArgumentException("The start and end not in same part or not in a same instance.");
        }

        Part part = getPartByPrimaryKey(startPrimaryKey);
        return parts.get(part.getId())
            .view(KVInstructions.id, KVInstructions.SCAN_OC, startPrimaryKey, endPrimaryKey, includeStart, includeEnd);
    }

    @Override
    public Iterator<KeyValue> keyValuePrefixScan(
        byte[] startPrimaryKey,
        byte[] endPrimaryKey,
        boolean includeStart,
        boolean includeEnd) {
        List<byte[]> keysInBytes = Arrays.asList(startPrimaryKey, endPrimaryKey);
        boolean isKeysOnSamePart = isKeysOnSamePart(keysInBytes);
        if (!isKeysOnSamePart) {
            throw new IllegalArgumentException("The start and end not in same part or not in a same instance.");
        }

        Part part = getPartByPrimaryKey(startPrimaryKey);
        return parts.get(part.getId())
            .view(KVInstructions.id, KVInstructions.SCAN_OC, startPrimaryKey, endPrimaryKey, includeStart, includeEnd);
    }

    /**
     * f prefix is 0xFFFF... will throw exception. user must handle the exception.
     * @param prefix key prefix
     * @return iterator
     */
    @Override
    public Iterator<KeyValue> keyValuePrefixScan(byte[] prefix) {
        return keyValuePrefixScan(prefix, ByteArrayUtils.increment(prefix), true, false);
    }

    @Override
    public boolean compute(byte[] startPrimaryKey, byte[] endPrimaryKey, List<byte[]> operations) {
        isValidRangeKey(startPrimaryKey, endPrimaryKey);
        int timestamp = -1;
        if (RocksUtils.ttlValid(this.ttl)) {
            timestamp = RocksUtils.getCurrentTimestamp();
        }

        List<byte[]> keysInBytes = Arrays.asList(startPrimaryKey, endPrimaryKey);
        boolean isKeysOnSamePart = isKeysOnSamePart(keysInBytes);
        if (!isKeysOnSamePart) {
            throw new IllegalArgumentException("The start and end not in same part or not in current instance.");
        }

        Part part = getPartByPrimaryKey(startPrimaryKey);
        parts.get(part.getId())
            .exec(OpInstructions.id, OpInstructions.COMPUTE_OC,
                startPrimaryKey, endPrimaryKey, operations, timestamp).join();

        return true;
    }

    private static void isValidRangeKey(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        if (endPrimaryKey != null && ByteArrayUtils.greatThan(startPrimaryKey, endPrimaryKey)) {
            throw new IllegalArgumentException("Invalid range key, start key should be less than end key");
        }
    }

    @Override
    public boolean delete(byte[] primaryKey) {
        Part part = getPartByPrimaryKey(primaryKey);
        if (part == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
            );
        }
        parts.get(part.getId()).exec(KVInstructions.id, KVInstructions.DEL_OC, primaryKey).join();
        return true;
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
                    isSuccess = parts.get(part.getId()).exec(KVInstructions.id, KVInstructions.DEL_OC,
                        primaryKeys.toArray()).join();
                    if (!isSuccess) {
                        log.error("Delete failed, part: {}, keysCnt: {}", part.getId(), keysInPart.get().size());
                    }
                } else {
                    log.warn("Delete failed, part: {}, keysCnt: 0", part.getId());
                }
            }
            return isSuccess;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
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
                boolean isOK = parts.get(part.getId()).exec(KVInstructions.id, KVInstructions.DEL_RANGE_OC,
                    keys.get(0), keys.get(1)).join();
                if (!isOK) {
                    isSuccess = false;
                    log.warn("Delete partition failed: part: " + part.getId() + ", keys: " + keys);
                }
            }
        } catch (Exception e) {
            log.error("Delete failed, startPrimaryKey: {}, endPrimaryKey: {}", startPrimaryKey, endPrimaryKey, e);
        }
        return isSuccess;
    }

    public Part getPartByPrimaryKey(byte[] primaryKey) {
        List<Part> partList = new ArrayList<>();
        startKeyPartMap.forEach((key, value) -> {
            if (lessThanOrEqual(key, primaryKey)) {
                if (value.getEnd() == null || lessThan(primaryKey, value.getEnd())) {
                    partList.add(value);
                }
            }
        });
        if (partList.size() == 0) {
            return null;
        } else {
            return partList.get(0);
        }
    }

    public Part getEndPartByPrimaryKey(byte[] primaryKey) {
        List<Part> partList = new ArrayList<>();
        startKeyPartMap.forEach((key, value) -> {
            if (lessThanOrEqual(key, primaryKey)) {
                if (value.getEnd() == null || lessThan(primaryKey, value.getEnd())) {
                    partList.add(value);
                }
            }
        });
        if (partList.size() == 0) {
            return null;
        } else {
            return partList.get(partList.size() - 1);
        }
    }

    class FullScanRawIterator implements Iterator<KeyValue> {
        protected Iterator<KeyValue> iterator;

        private final Iterator<Iterator<KeyValue>> partIterator;

        public FullScanRawIterator(Iterator<Iterator<KeyValue>> partIterator) {
            iterator = partIterator.next();
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

        @Override
        public KeyValue next() {
            return iterator.next();
        }
    }

    private Map<Part, List<byte[]>> groupKeysByPart(List<byte[]> primaryKeys) {
        Map<Part, List<byte[]>> result = new HashMap<>();
        for (byte[] primaryKey : primaryKeys) {
            Part part = getPartByPrimaryKey(primaryKey);
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
        Part startPart = getPartByPrimaryKey(startKey);
        Part endPart = getPartByPrimaryKey(endKey);

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
            if (ByteArrayUtils.lessThan(startKey, keyInList)) {
                startIndex = i - 1;
                break;
            }
        }

        for (int i = keyArraysList.size() - 1; i >= 0; i--) {
            byte[] keyInList = keyArraysList.get(i);
            if (ByteArrayUtils.greatThanOrEqual(endKey, keyInList)) {
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
                Part part = getPartByPrimaryKey(startKey);
                List<byte[]> list = Arrays.asList(startKey, keyInList);
                result.put(part, list);
            } else if (i == endIndex) {
                // last partition
                byte[] keyInList = keyArraysList.get(i);
                Part part = getPartByPrimaryKey(endKey);
                List<byte[]> list = Arrays.asList(keyInList, endKey);
                result.put(part, list);
            } else {
                // middle partition
                byte[] keyInList = keyArraysList.get(i);
                byte[] keyInListNext = keyArraysList.get(i + 1);
                Part part = getPartByPrimaryKey(keyInList);
                List<byte[]> list = Arrays.asList(keyInList, keyInListNext);
                result.put(part, list);
            }
        }
        return result;
    }

    @Override
    public KeyValue udfGet(byte[] primaryKey, String udfName, String functionName, int version) {
        try {
            KeyValue keyValue = new KeyValue(primaryKey, getValueByPrimaryKey(primaryKey));
            String cacheKey = udfName + "-" + version;
            MetaServiceApi metaServiceApi
                = ApiRegistry.getDefault().proxy(MetaServiceApi.class, CoordinatorConnector.defaultConnector());
            if (!codecMap.containsKey(cacheKey)) {
                TableDefinition tableDefinition = metaServiceApi.getTableDefinition(id);
                DingoKeyValueCodec codec =
                    new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
                definitionMap.put(cacheKey, tableDefinition);
                codecMap.put(cacheKey, codec);
            }
            KeyValueCodec codec = codecMap.get(cacheKey);
            Object[] record = codec.decode(keyValue);
            TableDefinition definition = definitionMap.get(cacheKey);
            LuaTable table = UdfUtils.getLuaTable(definition.getDingoSchema(), record);
            if (!globalsMap.containsKey(cacheKey)) {
                String function = metaServiceApi.getUDF(id, udfName, version);
                if (function != null) {
                    Globals globals = JsePlatform.standardGlobals();
                    globals.load(function).call();
                    globalsMap.put(cacheKey, globals);
                } else {
                    definitionMap.remove(cacheKey);
                    codecMap.remove(cacheKey);
                    throw new RuntimeException("UDF not register");
                }
            }
            Globals globals = globalsMap.get(cacheKey);
            LuaValue udf = globals.get(LuaValue.valueOf(functionName));
            LuaValue result = udf.call(table);
            record = UdfUtils.getObject(definition.getDingoSchema(), result);
            return codec.encode(record);
        } catch (Exception e) {
            throw new RuntimeException("UDF ERROR:", e);
        }
    }

    @Override
    public boolean udfUpdate(byte[] primaryKey, String udfName, String functionName, int version) {
        KeyValue updatedKeyValue = udfGet(primaryKey, udfName, functionName, version);
        return upsertKeyValue(updatedKeyValue);
    }

    private boolean isKeysOnSamePart(List<byte[]> keyArrayList) {
        if (keyArrayList == null || keyArrayList.size() < 1) {
            return false;
        }

        boolean isOnSamePart = true;
        Part part = getPartByPrimaryKey(keyArrayList.get(0));
        for (int i = 1; i < keyArrayList.size(); i++) {
            Part localPart = getPartByPrimaryKey(keyArrayList.get(i));
            /**
             * as Part has `Equal and HashCode`
             */
            if (!localPart.equals(part)) {
                isOnSamePart = false;
                break;
            }
        }

        return isOnSamePart;
    }
}
