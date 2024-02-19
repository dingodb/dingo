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

package io.dingodb.exec.operator;

import com.google.common.collect.Iterators;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnGetByIndexParam;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.common.util.Utils.calculatePrefixCount;

@Slf4j
public final class TxnGetByIndexOperator extends FilterProjectOperator {
    public static final TxnGetByIndexOperator INSTANCE = new TxnGetByIndexOperator();

    private TxnGetByIndexOperator() {
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Context context, Object[] tuple, Vertex vertex) {
        TxnGetByIndexParam param = vertex.getParam();
        byte[] keys = param.getCodec().encodeKeyPrefix(tuple, calculatePrefixCount(tuple));
        Iterator<KeyValue> localIterator = createScanLocalIterator(
            vertex.getTask().getTxnId(),
            context.getDistribution().getId(),
            param.getIndexTableId(),
            keys);
        StoreInstance store = Services.KV_STORE.getInstance(param.getIndexTableId(), context.getDistribution().getId());
        Iterator<KeyValue> storeIterator = store.txnScan(
            param.getScanTs(),
            new StoreInstance.Range(keys, keys, true, true),
            param.getTimeout());
        Iterator<Object[]> iterator = createMergedIterator(localIterator, storeIterator, param.getCodec());

        List<Object[]> objectList = new ArrayList<>();
        while (iterator.hasNext()) {
            Object[] objects = iterator.next();
            if (param.isLookup()) {
                Object[] val = lookUp(objects, param, vertex.getTask());
                if (val != null) {
                    objectList.add(val);
                }
            } else {
                objectList.add(transformTuple(objects, param));
            }
        }

        return objectList.iterator();
    }

    private static Object[] lookUp(Object[] tuples, TxnGetByIndexParam param, Task task) {
        CommonId txnId = task.getTxnId();
        TransactionType transactionType = task.getTransactionType();
        TupleMapping indices = param.getKeyMapping();
        Table tableDefinition = param.getTable();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
            MetaService.root().getRangeDistribution(tableDefinition.tableId);
        Object[] keyTuples = new Object[tableDefinition.getColumns().size()];
        for (int i = 0; i < indices.getMappings().length; i ++) {
            keyTuples[indices.get(i)] = tuples[i];
        }
        byte[] keys = param.getLookupCodec().encodeKey(keyTuples);
        CommonId regionId = PartitionService.getService(
                Optional.ofNullable(tableDefinition.getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(keys, ranges);

        keys = CodecService.getDefault().setId(keys, regionId.domain);
        Object[] local = createGetLocal(
            keys,
            txnId,
            regionId,
            param.getTableId(),
            param.getLookupCodec(),
            transactionType);
        if (local != null) {
            return local;
        }

        StoreInstance store = Services.KV_STORE.getInstance(param.getTableId(), regionId);
        return param.getLookupCodec().decode(store.txnGet(param.getScanTs(), keys, param.getTimeout()));
    }

    private static Object[] transformTuple(Object[] tuple, TxnGetByIndexParam param) {
        TupleMapping selection = param.getSelection();
        Table index = param.getIndex();
        Table table = param.getTable();
        Object[] response = new Object[table.getColumns().size()];
        List<Integer> selectedColumns = mapping(selection, table, index);
        for (int i = 0; i < selection.size(); i ++) {
            response[selection.get(i)] = tuple[selectedColumns.get(i)];
        }
        return response;
    }

    private static List<Integer> mapping(TupleMapping selection, Table td, Table index) {
        Integer[] mappings = new Integer[selection.size()];
        for (int i = 0; i < selection.size(); i ++) {
            Column column = td.getColumns().get(selection.get(i));
            mappings[i] = index.getColumns().indexOf(column);
        }
        return Arrays.asList(mappings);
    }

    private static Object[] createGetLocal(
        byte[] keys,
        CommonId txnId,
        CommonId partId,
        CommonId tableId,
        KeyValueCodec codec,
        TransactionType transactionType
    ) {
        byte[] txnIdByte = txnId.encode();
        byte[] partIdByte = partId.encode();
        byte[] tableIdByte = tableId.encode();
        int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
        byte[] dataKey = ByteUtils.encode(
            CommonId.CommonType.TXN_CACHE_DATA,
            keys,
            Op.PUTIFABSENT.getCode(),
            len,
            txnIdByte, tableIdByte, partIdByte);
        byte[] deleteKey = Arrays.copyOf(dataKey, dataKey.length);
        deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
        byte[] updateKey = Arrays.copyOf(dataKey, dataKey.length);
        updateKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
        List<byte[]> bytes = new ArrayList<>(3);
        bytes.add(dataKey);
        bytes.add(deleteKey);
        bytes.add(updateKey);
        StoreInstance store;
        store = Services.LOCAL_STORE.getInstance(tableId, partId);
        List<KeyValue> keyValues = store.get(bytes);
        if (keyValues != null && keyValues.size() > 0) {
            if (keyValues.size() > 1) {
                throw new RuntimeException(txnId + " Key is not existed than two in local store");
            }
            KeyValue value = keyValues.get(0);
            byte[] oldKey = value.getKey();
            if (oldKey[oldKey.length - 2] == Op.PUTIFABSENT.getCode()
                || oldKey[oldKey.length - 2] == Op.PUT.getCode()) {
                KeyValue keyValue = new KeyValue(keys, value.getValue());
                return codec.decode(keyValue);
            } else {
                if (transactionType == TransactionType.PESSIMISTIC) {
                    KeyValue keyValue = store.get(ByteUtils.getKeyByOp(
                        CommonId.CommonType.TXN_CACHE_LOCK,
                        Op.LOCK,
                        dataKey)
                    );
                    // first primary key
                    if (keyValue == null) {
                        return null;
                    }
                }
                return null;
            }
        }
        return null;
    }

    private static @Nullable KeyValue getNextValue(@NonNull Iterator<KeyValue> iterator) {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    private static Iterator<KeyValue> createScanLocalIterator(
        CommonId txnId,
        CommonId partId,
        CommonId indexId,
        byte[] keys
    ) {
        CodecService.getDefault().setId(keys, partId.domain);
        byte[] txnIdByte = txnId.encode();
        byte[] indexIdByte = indexId.encode();
        byte[] partIdByte = partId.encode();
        byte[] encodeStart = ByteUtils.encode(CommonId.CommonType.TXN_CACHE_DATA, keys, Op.NONE.getCode(),
            (txnIdByte.length + indexIdByte.length + partIdByte.length), txnIdByte, indexIdByte, partIdByte);
        byte[] encodeEnd = ByteUtils.encode(CommonId.CommonType.TXN_CACHE_DATA, keys, Op.NONE.getCode(),
            (txnIdByte.length + indexIdByte.length + partIdByte.length), txnIdByte, indexIdByte, partIdByte);
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(indexId, partId);
        return Iterators.transform(
            localStore.scan(new StoreInstance.Range(encodeStart, encodeEnd, true, true)),
            wrap(ByteUtils::mapping)::apply);
    }

    private static @NonNull Iterator<Object[]> createMergedIterator(
        Iterator<KeyValue> localKVIterator,
        Iterator<KeyValue> kvKVIterator,
        KeyValueCodec decoder
    ) {
        KeyValue kv1 = getNextValue(localKVIterator);
        KeyValue kv2 = getNextValue(kvKVIterator);

        final int pos = 9;
        List<KeyValue> mergedList = new ArrayList<>();
        List<KeyBytes> deletedList = new ArrayList<>();

        while (kv1 != null && kv2 != null) {
            byte[] key1 = kv1.getKey();
            byte[] key2 = kv2.getKey();
            int code = key1[key1.length - 2];
            if (ByteArrayUtils.lessThan(key1, key2, pos, key1.length - 2)) {
                if (
                    (code == Op.PUT.getCode() || code == Op.PUTIFABSENT.getCode())
                        && !deletedList.contains(new KeyBytes(key2))
                ) {
                    mergedList.add(kv1);
                    kv1 = getNextValue(localKVIterator);
                    continue;
                } else if (code == Op.DELETE.getCode()) {
                    deletedList.add(new KeyBytes(key1));
                    kv1 = getNextValue(localKVIterator);
                    continue;
                }
            }
            if (ByteArrayUtils.greatThan(key1, key2, pos, key1.length - 2)) {
                if (
                    (code == Op.PUT.getCode() || code == Op.PUTIFABSENT.getCode())
                        && !deletedList.contains(new KeyBytes(key2))
                ) {
                    mergedList.add(kv2);
                    kv2 = getNextValue(kvKVIterator);
                    continue;
                }
                if (code == Op.DELETE.getCode()) {
                    mergedList.add(kv2);
                    kv2 = getNextValue(kvKVIterator);
                    continue;
                }
            }
            if (ByteArrayUtils.compare(key1, key2, pos, key1.length - 2) == 0) {
                if (code == Op.DELETE.getCode()) {
                    kv1 = getNextValue(localKVIterator);
                    kv2 = getNextValue(kvKVIterator);
                    continue;
                }
                if ((code == Op.PUT.getCode() || code == Op.PUTIFABSENT.getCode())) {
                    mergedList.add(kv1);
                    kv1 = getNextValue(localKVIterator);
                    kv2 = getNextValue(kvKVIterator);
                }
            }
        }
        while (kv1 != null) {
            if (!mergedList.contains(kv1) && (kv1.getKey()[kv1.getKey().length - 2] != Op.DELETE.getCode())) {
                mergedList.add(kv1);
            }
            kv1 = getNextValue(localKVIterator);
        }
        while (kv2 != null) {
            byte[] key = kv2.getKey();
            if (!mergedList.contains(kv2) && !deletedList.contains(new KeyBytes(key))) {
                mergedList.add(kv2);
            }
            kv2 = getNextValue(kvKVIterator);
        }

        return Iterators.transform(mergedList.iterator(), wrap(decoder::decode)::apply);
    }

    @AllArgsConstructor
    private static class KeyBytes {
        private final byte[] key;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KeyBytes keyBytes = (KeyBytes) o;
            return ByteArrayUtils.compare(this.key, keyBytes.key, 9, this.key.length - 2) == 0;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(key);
        }
    }

}
