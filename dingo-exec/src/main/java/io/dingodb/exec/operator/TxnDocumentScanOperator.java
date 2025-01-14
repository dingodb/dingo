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
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.TxnDocumentScanFilterParam;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.exec.utils.TxnMergedIterator;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.DocumentSearchParameter;
import io.dingodb.store.api.transaction.data.Op;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class TxnDocumentScanOperator extends FilterProjectSourceOperator {
    public static final TxnDocumentScanOperator INSTANCE = new TxnDocumentScanOperator();

    public TxnDocumentScanOperator() {
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Vertex vertex) {
        TxnDocumentScanFilterParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("getByDocumentIndex");
        long start = System.currentTimeMillis();

        StoreInstance store = Services.KV_STORE.getInstance(param.getIndexTableId(), param.getPartId());
        Iterator<KeyValue> storeIterator = store.documentScanFilter(
            param.getScanTs(),
            DocumentSearchParameter.builder()
                .queryString(param.getQueryString())
                .queryUnlimited(true)
                .build()
        );

        profile.time(start);
        return Iterators.transform(storeIterator, tuples -> revMap(param.getCodec().decode(tuples), vertex));
    }

    public static Object[] revMap(Object[] tuple, Vertex vertex) {
        TxnDocumentScanFilterParam param = vertex.getParam();
        if (param.isLookup()) {
            return lookUp(tuple, param, vertex.getTask());
        } else {
            return transformTuple(tuple, param);
        }
    }

    public static Object[] lookUp(Object[] tuples, TxnDocumentScanFilterParam param, Task task) {
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

    private static Object[] transformTuple(Object[] tuple, TxnDocumentScanFilterParam param) {
        TupleMapping selection = param.getSelection();
        Table table = param.getTable();
        Object[] response = new Object[table.getColumns().size()];
        List<Integer> selectedColumns = param.getMapList();
        for (int i = 0; i < selection.size(); i ++) {
            response[selection.get(i)] = tuple[selectedColumns.get(i)];
        }
        return response;
    }

    public static Object[] createGetLocal(
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
        if (keyValues != null && !keyValues.isEmpty()) {
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

    protected Iterator<KeyValue> createScanLocalIterator(
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

    public static @NonNull Iterator<Object[]> createMergedIterator(
        Iterator<KeyValue> localKVIterator,
        Iterator<KeyValue> kvKVIterator,
        KeyValueCodec decoder
    ) {
        return new TxnMergedIterator(localKVIterator, kvKVIterator, decoder);
    }

}
