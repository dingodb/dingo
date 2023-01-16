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

package io.dingodb.server.executor.store;

import io.dingodb.common.CommonId;
import io.dingodb.common.codec.DingoKeyValueCodec;
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.store.Part;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.IndexStatus;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.UdfUtils;
import io.dingodb.common.util.Utils;
import io.dingodb.mpu.core.VCore;
import io.dingodb.mpu.instruction.KVInstructions;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.CodeUDFApi;
import io.dingodb.server.api.ExecutorApi;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.executor.index.IndexExecutor;
import io.dingodb.server.executor.sidebar.TableSidebar;
import io.dingodb.server.executor.sidebar.TableStatus;
import io.dingodb.server.executor.store.instruction.OpInstructions;
import io.dingodb.server.protocol.meta.TablePartStats.ApproximateStats;
import lombok.extern.slf4j.Slf4j;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dingodb.common.codec.PrimitiveCodec.encodeInt;
import static io.dingodb.common.util.ByteArrayUtils.lessThan;
import static io.dingodb.common.util.ByteArrayUtils.lessThanOrEqual;
import static io.dingodb.common.util.ByteArrayUtils.unsliced;

@Slf4j
public class StoreInstance implements io.dingodb.store.api.StoreInstance {

    private final CommonId id;

    private final TableSidebar tableSidebar;

    private final NavigableMap<byte[], Part> startKeyPartMap = new ConcurrentSkipListMap<>(ByteArrayUtils::compare);

    private Map<String, Globals> globalsMap = new HashMap<>();

    private Map<String, TableDefinition> definitionMap = new HashMap<>();

    private Map<String, KeyValueCodec> codecMap = new HashMap<>();

    private IndexExecutor indexExecutor;

    Lock lock = new ReentrantLock();

    private Set<KeyRowLock> rowLock = new HashSet<>();

    public StoreInstance(TableSidebar tableSidebar) {
        this.tableSidebar = tableSidebar;
        this.id = tableSidebar.tableId;
        this.indexExecutor = new IndexExecutor(this.id);
    }

    public ApproximateStats approximateStats(VCore core) {
        return new ApproximateStats(ByteArrayUtils.EMPTY_BYTES, null, approximateCount(core),
            approximateSize(core));
    }

    public long approximateCount(VCore core) {
        return core.storage.approximateCount();
    }

    public long approximateSize(VCore core) {
        return core.storage.approximateSize();
    }

    public void onPartAvailable(Part part) {
        startKeyPartMap.put(part.getStart(), part);
    }

    public void onPartDisable(byte[] start) {
        startKeyPartMap.remove(start);
    }

    @Override
    public boolean exist(byte[] primaryKey) {
        Part part = getPartByPrimaryKey(primaryKey);
        if (part == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
            );
        }
        return tableSidebar
            .getPartition(part.getId()).view(KVInstructions.id, KVInstructions.GET_OC, primaryKey) != null;
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
        if (tableSidebar.ttl()) {
            tableSidebar.getPartition(part.getId()).exec(
                KVInstructions.id,
                KVInstructions.SET_OC,
                primaryKey,
                encodeInt(Utils.currentSecond(), unsliced(row, 0, row.length + 4), row.length, false)
            ).join();
        } else {
            tableSidebar.getPartition(part.getId()).exec(KVInstructions.id, KVInstructions.SET_OC, primaryKey, row)
                .join();
        }

        return true;
    }

    @Override
    public boolean upsertKeyValue(KeyValue row) {
        return upsertKeyValue(row.getPrimaryKey(), row.getValue());
    }

    @Override
    public boolean upsertKeyValue(List<KeyValue> rows) {
        List<byte[]> rowKeyList = rows.stream().map(KeyValue::getPrimaryKey).collect(Collectors.toList());
        boolean isAllKeyOnSamePart = isKeysOnSamePart(rowKeyList);
        if (!isAllKeyOnSamePart) {
            log.warn("The input keys are not in a same instance.");
            throw new IllegalArgumentException("The key not in current instance.");
        }

        if (tableSidebar.ttl()) {
            int time = Utils.currentSecond();
            rows = rows.stream()
                .filter(Objects::nonNull)
                .map(kv -> new KeyValue(kv.getKey(), encodeInt(time, unsliced(kv.getValue(), -4), false)))
                .collect(Collectors.toList());
        }

        Part part = getPartByPrimaryKey(rows.get(0).getPrimaryKey());
        tableSidebar.getPartition(part.getId()).exec(
            KVInstructions.id, KVInstructions.SET_BATCH_OC,
            rows.stream().flatMap(kv -> Stream.of(kv.getPrimaryKey(), kv.getValue())).toArray()
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
        return tableSidebar.getPartition(part.getId()).view(KVInstructions.id, KVInstructions.GET_OC, primaryKey);
    }

    @Override
    public void deletePart(Part part) {
        // 1 clean parts
        // 2 clean startKeyPartMap
        //startKeyPartMap.remove(part.getStart());
        //parts.remove(part.getId()).destroy();
    }

    @Override
    public long countOrDeletePart(byte[] startKey, boolean doDeleting) {
        Part part = getPartByPrimaryKey(startKey);
        if (part == null) {
            log.warn("Count or delete store by part start key. but find start key:{} not in any part",
                Arrays.toString(startKey));
            return 0;
        }

        if (doDeleting) {
            return tableSidebar.getPartition(part.getId()).exec(
                KVInstructions.id,
                KVInstructions.DEL_RANGE_WITH_COUNT_OC,
                ByteArrayUtils.EMPTY_BYTES,
                null
            ).join();
        }
        return tableSidebar.getPartition(part.getId()).view(KVInstructions.id, KVInstructions.COUNT_OC, null, null);
    }

    @Override
    public long countDeleteByRange(byte[] startKey, byte[] endKey, boolean includeStart, boolean includeEnd) {
        if (!isKeysOnSamePart(Arrays.asList(startKey, endKey))) {
            throw new IllegalArgumentException("The start and end not in same part or not in current instance.");
        }

        byte[] adjustStartKey = (startKey == null) ? null :
            includeStart ? startKey : ByteArrayUtils.increment(startKey);
        byte[] adjustEndKey = (endKey == null) ? null : includeEnd ? ByteArrayUtils.increment(endKey) : endKey;

        Part part = getPartByPrimaryKey(startKey);
        return tableSidebar.getPartition(part.getId()).exec(
            KVInstructions.id, KVInstructions.DEL_RANGE_WITH_COUNT_OC, adjustStartKey, adjustEndKey).join();
    }

    @Override
    public KeyValue getKeyValueByPrimaryKey(byte[] primaryKey) {
        Part part = getPartByPrimaryKey(primaryKey);
        if (part == null) {
            throw new IllegalArgumentException(
                "The primary key " + Arrays.toString(primaryKey) + " not in current instance."
            );
        }
        return tableSidebar.getPartition(part.getId()).view(KVInstructions.id, KVInstructions.GET_OC, primaryKey);
    }

    @Override
    public List<KeyValue> getKeyValueByPrimaryKeys(List<byte[]> primaryKeys) {
        if (!isKeysOnSamePart(primaryKeys)) {
            throw new IllegalArgumentException("The primary key list not in same part.");
        }
        Part part = getPartByPrimaryKey(primaryKeys.get(0));
        return tableSidebar
            .getPartition(part.getId()).view(KVInstructions.id, KVInstructions.GET_BATCH_OC, primaryKeys);
    }

    @Override
    public Iterator<KeyValue> keyValueScan() {
        List<Iterator<KeyValue>> iterators = new ArrayList<>();
        startKeyPartMap.values().stream()
            .map(Part::getId)
            .map(tableSidebar::getPartition)
            .forEach(store -> {
                iterators.add(store.view(KVInstructions.id, KVInstructions.SCAN_OC));
            });
        return new FullScanRawIterator(iterators.iterator());
    }

    @Override
    public Iterator<KeyValue> keyValueScan(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        isValidRangeKey(startPrimaryKey, endPrimaryKey);

        if (!isKeysOnSamePart(Arrays.asList(startPrimaryKey, endPrimaryKey))) {
            throw new IllegalArgumentException("The start and end not in same part or not in a same instance.");
        }

        Part part = getPartByPrimaryKey(startPrimaryKey);
        return tableSidebar
            .getPartition(part.getId()).view(KVInstructions.id, KVInstructions.SCAN_OC, startPrimaryKey, endPrimaryKey);
    }

    @Override
    public Iterator<KeyValue> keyValueScan(
        byte[] startPrimaryKey,
        byte[] endPrimaryKey,
        boolean includeStart,
        boolean includeEnd) {
        isValidRangeKey(startPrimaryKey, endPrimaryKey);

        if (endPrimaryKey != null && !isKeysOnSamePart(Arrays.asList(startPrimaryKey, endPrimaryKey))) {
            throw new IllegalArgumentException("The start and end not in same part or not in a same instance.");
        }

        Part part = getPartByPrimaryKey(startPrimaryKey);
        return tableSidebar.getPartition(part.getId())
            .view(KVInstructions.id, KVInstructions.SCAN_OC, startPrimaryKey, endPrimaryKey, includeStart, includeEnd);
    }

    @Override
    public Iterator<KeyValue> keyValuePrefixScan(
        byte[] startPrimaryKey,
        byte[] endPrimaryKey,
        boolean includeStart,
        boolean includeEnd) {
        if (endPrimaryKey != null || !isKeysOnSamePart(Arrays.asList(startPrimaryKey, endPrimaryKey))) {
            throw new IllegalArgumentException("The start and end not in same part or not in a same instance.");
        }

        Part part = getPartByPrimaryKey(startPrimaryKey);
        return tableSidebar.getPartition(part.getId())
            .view(KVInstructions.id, KVInstructions.SCAN_OC, startPrimaryKey, endPrimaryKey, includeStart, includeEnd);
    }

    /**
     * f prefix is 0xFFFF... will throw exception. user must handle the exception.
     *
     * @param prefix key prefix
     * @return iterator
     */
    @Override
    public Iterator<KeyValue> keyValuePrefixScan(byte[] prefix) {
        return keyValuePrefixScan(prefix, prefix, true, true);
    }

    @Override
    public Object compute(List<byte[]> startPrimaryKeys, List<byte[]> endPrimaryKeys, byte[] op, boolean readOnly) {
        int timestamp = -1;
        if (tableSidebar.ttl()) {
            timestamp = Utils.currentSecond();
        }

        Part part = null;
        List<byte[]> endList = new ArrayList<>();
        for (int i = 0; i < startPrimaryKeys.size(); i++) {
            part = getPartByPrimaryKey(startPrimaryKeys.get(i));
            if (part == null) {
                throw new IllegalArgumentException("The start and end not in current instance.");
            }
            byte[] endPrimaryKey = null;
            if (endPrimaryKeys != null && endPrimaryKeys.size() == startPrimaryKeys.size()) {
                endPrimaryKey = endPrimaryKeys.get(i);
            }
            if (endPrimaryKey == null) {
                endPrimaryKey = part.getEnd();
            } else if (getPartByPrimaryKey(endPrimaryKey) != part) {
                throw new IllegalArgumentException("The start and end not in same part or not in current instance.");
            }
            endList.add(endPrimaryKey);
            endPrimaryKeys = endList;
        }
        if (readOnly) {
            return tableSidebar.getPartition(part.getId()).view(
                OpInstructions.id, OpInstructions.COMPUTE_OC, startPrimaryKeys, endPrimaryKeys, op, timestamp);
        }

        return tableSidebar.getPartition(part.getId())
            .exec(OpInstructions.id, OpInstructions.COMPUTE_OC, startPrimaryKeys, endPrimaryKeys, op, timestamp)
            .join();
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
        tableSidebar.getPartition(part.getId()).exec(KVInstructions.id, KVInstructions.DEL_OC, primaryKey).join();
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
                    isSuccess = tableSidebar.getPartition(part.getId()).exec(KVInstructions.id, KVInstructions.DEL_OC,
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
                boolean isOK = tableSidebar.getPartition(part.getId()).exec(
                    KVInstructions.id, KVInstructions.DEL_RANGE_OC, keys.get(0), ByteArrayUtils.increment(keys.get(1))
                ).join();
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
            if ((primaryKey == null && value.getEnd() == null)
                || (primaryKey != null && lessThanOrEqual(key, primaryKey))) {
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
                = ApiRegistry.getDefault().proxy(MetaServiceApi.class, CoordinatorConnector.getDefault());
            if (!codecMap.containsKey(cacheKey)) {
                TableDefinition tableDefinition = metaServiceApi.getTableDefinition(tableSidebar.tableId);
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
                String function = ApiRegistry.getDefault()
                    .proxy(CodeUDFApi.class, CoordinatorConnector.getDefault()).get(udfName, version);
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
        if (keyArrayList == null || keyArrayList.isEmpty()) {
            return false;
        }

        if (keyArrayList.size() == 2 && keyArrayList.contains(null)) {
            return true;
        }

        Part part = getPartByPrimaryKey(keyArrayList.get(0));
        for (int i = 1; i < keyArrayList.size(); i++) {
            Part localPart = getPartByPrimaryKey(keyArrayList.get(i));
            // as Part has `Equal and HashCode`
            if (!localPart.equals(part)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean insert(Object[] row) {
        if (!tableSidebar.getStatus().equals(TableStatus.RUNNING)) {
            throw new RuntimeException("Table is not in running status");
        }

        TableDefinition tableDefinition = tableSidebar.getDefinition();
        int tableDefinitionVersion = tableDefinition.getVersion();

        KeyValue oriKV = indexExecutor.getOriKV(row, tableDefinition);
        KeyRowLock keyRowLock = new KeyRowLock(oriKV.getPrimaryKey());
        lock.lock();
        if (rowLock.contains(keyRowLock)) {
            lock.unlock();
            return false;
        }
        rowLock.add(keyRowLock);
        lock.unlock();

        try {
            ExecutorApi unfinishExecutorApi = indexExecutor
                .getExecutor(indexExecutor.getUnfinishKV(oriKV).getKey(), tableDefinition);
            ExecutorApi finishedExecutorApi = indexExecutor
                .getExecutor(indexExecutor.getFinishedKV(oriKV).getKey(), tableDefinition);

            if (!unfinishExecutorApi
                .upsertKeyValue(null, null, id, indexExecutor.getUnfinishKV(oriKV))) {
                return false;
            }

            Set<String> indexNames = tableDefinition.getIndexes().keySet();
            for (String indexName : indexNames) {
                if (!indexExecutor.insertIndex(row, tableDefinition, indexName)) {
                    return false;
                }
            }

            TableDefinition currentTd = tableSidebar.getDefinition();
            if (currentTd.getVersion() != tableDefinitionVersion) {
                throw new RuntimeException("table definition changed");
            }
            if (!finishedExecutorApi
                .upsertKeyValue(null, null, id, indexExecutor.getFinishedKV(oriKV))) {
                return false;
            }
            if (!unfinishExecutorApi
                .delete(null, null, id, indexExecutor.getUnfinishKV(oriKV).getKey())) {
                return false;
            }
            return true;
        } finally {
            indexExecutor.getFinishedKV(oriKV);
            rowLock.remove(keyRowLock);
        }
    }

    @Override
    public boolean update(Object[] row) {
        if (!tableSidebar.getStatus().equals(TableStatus.RUNNING)) {
            throw new RuntimeException("Table is not in running status");
        }

        TableDefinition tableDefinition = tableSidebar.getDefinition();
        int tableDefinitionVersion = tableDefinition.getVersion();

        KeyValue oriKV = indexExecutor.getOriKV(row, tableDefinition);
        ExecutorApi finishedExecutorApi = indexExecutor
            .getExecutor(indexExecutor.getFinishedKV(oriKV).getKey(), tableDefinition);
        byte[] oldValue = finishedExecutorApi
            .getValueByPrimaryKey(null, null, id, indexExecutor.getFinishedKV(oriKV).getKey());
        if (oldValue == null) {
            return false;
        }
        KeyValue oldKV = new KeyValue(oriKV.getKey(), oldValue);

        KeyRowLock keyRowLock = new KeyRowLock(oriKV.getPrimaryKey());
        lock.lock();
        if (rowLock.contains(keyRowLock)) {
            lock.unlock();
            return false;
        }
        rowLock.add(keyRowLock);
        lock.unlock();

        try {
            ExecutorApi unfinishExecutorApi = indexExecutor
                .getExecutor(indexExecutor.getUnfinishKV(oriKV).getKey(), tableDefinition);
            ExecutorApi oldDeleteExecutorApi = indexExecutor
                .getExecutor(indexExecutor.getDeleteKV(oldKV).getKey(), tableDefinition);

            Object[] oldRow = indexExecutor.getRow(oldKV, tableDefinition);

            if (!unfinishExecutorApi
                .upsertKeyValue(null, null, id, indexExecutor.getUnfinishKV(oriKV))) {
                return false;
            }

            if (!oldDeleteExecutorApi
                .upsertKeyValue(null, null, id, indexExecutor.getDeleteKV(oldKV))) {
                return false;
            }

            Set<String> indexNames = tableDefinition.getIndexes().keySet();
            for (String indexName : indexNames) {
                indexExecutor.deleteFromIndex(oldRow, tableDefinition, indexName);
                indexExecutor.insertIndex(row, tableDefinition, indexName);
            }

            TableDefinition currentTd = tableSidebar.getDefinition();
            if (currentTd.getVersion() != tableDefinitionVersion) {
                throw new RuntimeException("table definition changed");
            }
            if (!finishedExecutorApi
                .delete(null, null, id, indexExecutor.getFinishedKV(oriKV).getKey())) {
                return false;
            }
            if (!oldDeleteExecutorApi
                .delete(null, null, id, indexExecutor.getDeleteKV(oldKV).getKey())) {
                return false;
            }
            if (!finishedExecutorApi
                .upsertKeyValue(null, null, id, indexExecutor.getFinishedKV(oriKV))) {
                return false;
            }
            if (!unfinishExecutorApi
                .delete(null, null, id, indexExecutor.getUnfinishKV(oriKV).getKey())) {
                return false;
            }
            return true;
        } finally {
            indexExecutor.getFinishedKV(oriKV);
            rowLock.remove(keyRowLock);
        }
    }

    @Override
    public boolean delete(Object[] row) {
        if (!tableSidebar.getStatus().equals(TableStatus.RUNNING)) {
            throw new RuntimeException("Table is not in running status");
        }

        TableDefinition tableDefinition = tableSidebar.getDefinition();
        int tableDefinitionVersion = tableDefinition.getVersion();

        KeyValue oriKV = indexExecutor.getOriKV(row, tableDefinition);
        KeyRowLock keyRowLock = new KeyRowLock(oriKV.getPrimaryKey());
        lock.lock();
        if (rowLock.contains(keyRowLock)) {
            lock.unlock();
            return false;
        }
        rowLock.add(keyRowLock);
        lock.unlock();

        try {
            ExecutorApi finishedExecutorApi = indexExecutor
                .getExecutor(indexExecutor.getFinishedKV(oriKV).getKey(), tableDefinition);
            ExecutorApi deleteExecutorApi = indexExecutor
                .getExecutor(indexExecutor.getDeleteKV(oriKV).getKey(), tableDefinition);

            if (finishedExecutorApi
                .getValueByPrimaryKey(null, null, id, indexExecutor.getFinishedKV(oriKV).getKey()) == null) {
                return false;
            }

            if (!deleteExecutorApi
                .upsertKeyValue(null, null, id, indexExecutor.getDeleteKV(oriKV))) {
                return false;
            }

            Set<String> indexNames = tableDefinition.getIndexes().keySet();
            for (String indexName : indexNames) {
                if (!indexExecutor.deleteFromIndex(row, tableDefinition, indexName)) {
                    return false;
                }
            }

            TableDefinition currentTd = tableSidebar.getDefinition();

            if (currentTd.getVersion() != tableDefinitionVersion) {
                throw new RuntimeException("table definition changed");
            }

            if (!finishedExecutorApi
                .delete(null, null, id, indexExecutor.getFinishedKV(oriKV).getKey())) {
                return false;
            }
            if (!deleteExecutorApi
                .delete(null, null, id, indexExecutor.getDeleteKV(oriKV).getKey())) {
                return false;
            }

            return true;
        } finally {
            indexExecutor.getFinishedKV(oriKV);
            rowLock.remove(keyRowLock);
        }
    }

    @Override
    public List<Object[]> select(Object[] row, boolean[] hasData) {
        if (!tableSidebar.getStatus().equals(TableStatus.RUNNING)) {
            throw new RuntimeException("Table is not in running status");
        }
        TableDefinition tableDefinition = tableSidebar.getDefinition();
        if (log.isDebugEnabled()) {
            log.debug("Definition of table = {}", tableDefinition);
        }
        List<ColumnDefinition> tableColumns = tableDefinition.getColumns();
        List<String> columnNames = new ArrayList<>();
        for (int i = 0; i < hasData.length; i++) {
            if (hasData[i]) {
                columnNames.add(tableColumns.get(i).getName());
            }
        }
        List<Index> indices = tableDefinition.getIndexesByEqualsColumnNames(columnNames);
        if (indices.size() == 0) {
            throw new RuntimeException("no index found");
        }
        return indexExecutor.getRowByIndex(row, tableDefinition, indices.get(0).getName());
    }

    public void reboot() {
        if (!tableSidebar.getStatus().equals(TableStatus.STARTING)) {
            throw new RuntimeException("Table is not in starting status");
        }

        TableDefinition tableDefinition = tableSidebar.getDefinition();

        tableDefinition.getDeletedIndexes().forEach(tableSidebar::dropIndex);

        Set<String> indexNames = tableDefinition.getIndexes().keySet();
        List<Object[]> deleteRecords = indexExecutor.getDeleteRecords();
        for (Object[] deleteRow : deleteRecords) {
            KeyValue oriKV = indexExecutor.getOriKV(deleteRow, tableDefinition);
            ExecutorApi deleteExecutorApi = indexExecutor
                .getExecutor(indexExecutor.getDeleteKV(oriKV).getKey(), tableDefinition);
            ExecutorApi finishedExecutorApi = indexExecutor
                .getExecutor(indexExecutor.getFinishedKV(oriKV).getKey(), tableDefinition);
            for (String indexName : indexNames) {
                indexExecutor.deleteFromIndex(deleteRow, tableDefinition, indexName);
            }
            finishedExecutorApi.delete(null, null, id,
                indexExecutor.getFinishedKV(oriKV).getPrimaryKey());
            deleteExecutorApi.delete(null, null, id,
                indexExecutor.getDeleteKV(oriKV).getPrimaryKey());
        }

        List<Object[]> unfinishRecords = indexExecutor.getUnfinishRecords();
        for (Object[] unfinishRow : unfinishRecords) {
            KeyValue oriKV = indexExecutor.getOriKV(unfinishRow, tableDefinition);
            ExecutorApi unfinishExecutorApi = indexExecutor
                .getExecutor(indexExecutor.getUnfinishKV(oriKV).getKey(), tableDefinition);
            ExecutorApi finishedExecutorApi = indexExecutor
                .getExecutor(indexExecutor.getFinishedKV(oriKV).getKey(), tableDefinition);
            for (String indexName : indexNames) {
                indexExecutor.insertIndex(unfinishRow, tableDefinition, indexName);
            }
            finishedExecutorApi.upsertKeyValue(null, null,
                id, indexExecutor.getFinishedKV(oriKV));
            unfinishExecutorApi.delete(null, null,
                id, indexExecutor.getUnfinishKV(oriKV).getPrimaryKey());
        }

        List<String> busyIndexNames = tableDefinition.getBusyIndexes();
        if (busyIndexNames.size() > 0) {
            List<Object[]> finishedRecords = indexExecutor.getFinishedRecords();
            for (String indexName : busyIndexNames) {
                for (Object[] row : finishedRecords) {
                    indexExecutor.insertIndex(row, tableDefinition, indexName);
                }
                Index index = tableDefinition.getIndexes().get(indexName);
                index.setStatus(IndexStatus.NORMAL);
                tableSidebar.updateDefinition(tableDefinition);
            }
        }
    }

    private class KeyRowLock {
        byte[] primaryKey;

        public KeyRowLock(byte[] primaryKey) {
            this.primaryKey = primaryKey;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj != null && !(obj instanceof KeyRowLock)) {
                return false;
            }
            return Arrays.equals(primaryKey, ((KeyRowLock) obj).primaryKey);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(primaryKey);
        }
    }
}

