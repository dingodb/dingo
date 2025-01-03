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

import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.store.KeyValue;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnPartDeleteParam;
import io.dingodb.exec.transaction.base.TxnPartData;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.exec.utils.OpStateUtils;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class TxnPartDeleteOperator extends PartModifyOperator {
    public static final TxnPartDeleteOperator INSTANCE = new TxnPartDeleteOperator();

    private TxnPartDeleteOperator() {
    }

    @Override
    protected boolean pushTuple(Context context, Object[] tuple, Vertex vertex) {
        TxnPartDeleteParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("partDelete");
        long start = System.currentTimeMillis();
        param.setContext(context);
        CommonId txnId = vertex.getTask().getTxnId();
        CommonId tableId = param.getTableId();
        CommonId partId = context.getDistribution().getId();
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
        KeyValueCodec codec = param.getCodec();
        boolean isVector = false;
        boolean isDocument = false;
        if (context.getIndexId() != null) {
            Table indexTable = (Table) TransactionManager.getIndex(txnId, context.getIndexId());
            if (indexTable == null) {
                LogUtils.error(log, "[ddl] TxnPartDelete get index table null, indexId:{}", context.getIndexId());
                return false;
            }
            if (!OpStateUtils.allowDeleteOnly(indexTable.getSchemaState())) {
                return true;
            }
            List<Integer> columnIndices = param.getTable().getColumnIndices(indexTable.columns.stream()
                .map(Column::getName)
                .collect(Collectors.toList()));
            tableId = context.getIndexId();
            if (!param.isPessimisticTxn()) {
                Object defaultVal = null;
                if (columnIndices.contains(-1)) {
                    Column addColumn = indexTable.getColumns().stream()
                        .filter(column -> column.getSchemaState() != SchemaState.SCHEMA_PUBLIC)
                        .findFirst().orElse(null);
                    if (addColumn != null) {
                        defaultVal = addColumn.getDefaultVal();
                    }
                }
                Object[] finalTuple = tuple;
                Object finalDefaultVal = defaultVal;
                tuple = columnIndices.stream().map(i -> {
                    if (i == -1) {
                        return finalDefaultVal;
                    }
                    return finalTuple[i];
                }).toArray();
            }
            IndexTable index = (IndexTable) TransactionManager.getIndex(txnId, tableId);
            if (index.indexType.isVector) {
                isVector = true;
            }
            if (index.indexType == IndexType.DOCUMENT) {
                isDocument = true;
            }
            localStore = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
            codec = CodecService.getDefault().createKeyValueCodec(
                indexTable.getCodecVersion(), indexTable.version, indexTable.tupleType(), indexTable.keyMapping()
            );
        }
        byte[] keys = wrap(codec::encodeKey).apply(tuple);
        CodecService.getDefault().setId(keys, partId.domain);
        byte[] txnIdBytes = vertex.getTask().getTxnId().encode();
        byte[] tableIdBytes = tableId.encode();
        byte[] partIdBytes = partId.encode();
        byte[] jobIdByte = vertex.getTask().getJobId().encode();
        int len = txnIdBytes.length + tableIdBytes.length + partIdBytes.length;
        if (param.isPessimisticTxn()) {
            byte[] keyValueKey = keys;
            // dataKeyValue   [10_txnId_tableId_partId_a_delete, value]
            byte[] dataKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_DATA,
                keyValueKey,
                Op.DELETE.getCode(),
                len,
                txnIdBytes,
                tableIdBytes,
                partIdBytes
            );
            byte[] insertKey = Arrays.copyOf(dataKey, dataKey.length);
            insertKey[insertKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
            byte[] updateKey = Arrays.copyOf(dataKey, dataKey.length);
            updateKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
            List<byte[]> bytes = new ArrayList<>(3);
            bytes.add(dataKey);
            bytes.add(insertKey);
            bytes.add(updateKey);
            List<KeyValue> keyValues = localStore.get(bytes);
            if (keyValues != null && keyValues.size() > 0) {
                if (keyValues.size() > 1) {
                    throw new RuntimeException(txnId + " PrimaryKey is not existed than two in local store");
                }
                KeyValue value = keyValues.get(0);
                byte[] oldKey = value.getKey();
                LogUtils.debug(log, "{}, repeat key :{}", txnId, Arrays.toString(oldKey));
                if (oldKey[oldKey.length - 2] == Op.DELETE.getCode()) {
                    profile.time(start);
                    return true;
                }
                localStore.delete(oldKey);
                // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
                byte[] extraKey = ByteUtils.encode(
                    CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                    keyValueKey,
                    oldKey[oldKey.length - 2],
                    len,
                    jobIdByte,
                    tableIdBytes,
                    partIdBytes
                );
                KeyValue extraKeyValue;
                KeyValue dataKeyValue;
                if (value.getValue() == null) {
                    // delete
                    extraKeyValue = new KeyValue(extraKey, null);
                    dataKeyValue = new KeyValue(dataKey, null);
                } else {
                    extraKeyValue = new KeyValue(extraKey, Arrays.copyOf(value.getValue(), value.getValue().length));
                    dataKeyValue = new KeyValue(dataKey, Arrays.copyOf(value.getValue(), value.getValue().length));
                }
                localStore.put(extraKeyValue);
                localStore.delete(insertKey);
                localStore.delete(updateKey);
                vertex.getTask().getPartData().put(
                    new TxnPartData(tableId, partId),
                    (!isVector && !isDocument)
                );
                // write data
                if (localStore.put(dataKeyValue) && context.getIndexId() == null) {
                    param.inc();
                }
            } else {
                byte[] rollBackKey = ByteUtils.getKeyByOp(
                    CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK,
                    Op.DELETE,
                    dataKey
                );
                // first lock and kvGet is null
                if (localStore.get(rollBackKey) != null) {
                    vertex.getTask().getPartData().put(
                        new TxnPartData(tableId, partId),
                        (!isVector && !isDocument)
                    );
                    profile.time(start);
                    return true;
                } else {
                    rollBackKey = ByteUtils.getKeyByOp(
                        CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.PUT, dataKey
                    );
                    if (localStore.get(rollBackKey) != null) {
                        localStore.delete(rollBackKey);
                    }
                    KeyValue kv = wrap(codec::encode).apply(tuple);
                    if (kv == null) {
                        // Delete non-existent keys
                        kv = new KeyValue(keys, new byte[]{});
                    }
                    CodecService.getDefault().setId(kv.getKey(), partId.domain);
                    // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
                    byte[] extraKey = ByteUtils.encode(
                        CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                        keyValueKey,
                        Op.NONE.getCode(),
                        len,
                        jobIdByte,
                        tableIdBytes,
                        partIdBytes
                    );
                    localStore.put(new KeyValue(extraKey, Arrays.copyOf(kv.getValue(), kv.getValue().length)));
                    vertex.getTask().getPartData().put(
                        new TxnPartData(tableId, partId),
                        (!isVector && !isDocument)
                    );
                    // write data
                    kv.setKey(dataKey);
                    if (localStore.put(kv)
                        && context.getIndexId() == null
                    ) {
                        param.inc();
                    }
                }
            }
        } else {
            KeyValue kv = wrap(codec::encode).apply(tuple);
            CodecService.getDefault().setId(keys, partId.domain);
            byte[] resultKeys = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_DATA,
                keys,
                Op.DELETE.getCode(),
                (txnIdBytes.length + tableIdBytes.length + partIdBytes.length),
                txnIdBytes,
                tableIdBytes,
                partIdBytes
            );
            KeyValue keyValue = new KeyValue(resultKeys, kv.getValue());
            Op op = Op.NONE;
            byte[] insertKey = Arrays.copyOf(keyValue.getKey(), keyValue.getKey().length);
            insertKey[insertKey.length - 2] = (byte) Op.PUT.getCode();
            if (localStore.get(insertKey) != null) {
                op = Op.PUT;
            }
            localStore.delete(insertKey);
            insertKey[insertKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
            if (localStore.get(insertKey) != null) {
                op = Op.PUTIFABSENT;
            }
            localStore.delete(insertKey);
            // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
            byte[] extraKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                keys,
                op.getCode(),
                len,
                jobIdByte,
                tableIdBytes,
                partIdBytes
            );
            localStore.put(new KeyValue(extraKey, Arrays.copyOf(keyValue.getValue(), keyValue.getValue().length)));
            vertex.getTask().getPartData().put(
                new TxnPartData(tableId, partId),
                (!isVector && !isDocument)
            );
            if (localStore.put(keyValue) && context.getIndexId() == null) {
                param.inc();
                context.addKeyState(true);
            }
        }
        profile.time(start);
        return true;
    }

}
