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
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnPartUpdateParam;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class TxnPartUpdateOperator extends PartModifyOperator {
    public static final TxnPartUpdateOperator INSTANCE = new TxnPartUpdateOperator();

    private TxnPartUpdateOperator() {
    }

    @Override
    protected boolean pushTuple(Context context, Object[] tuple, Vertex vertex) {
        TxnPartUpdateParam param = vertex.getParam();
        param.setContext(context);
        DingoType schema = param.getSchema();
        TupleMapping mapping = param.getMapping();
        List<SqlExpr> updates = param.getUpdates();

        int tupleSize = schema.fieldCount();
        Object[] newTuple = Arrays.copyOf(tuple, tupleSize);
        boolean updated = false;
        int i = 0;
        try {
            for (i = 0; i < mapping.size(); ++i) {
                Object newValue = updates.get(i).eval(tuple);
                int index = mapping.get(i);
                if ((newTuple[index] == null && newValue != null)
                    || (newTuple[index] != null && !newTuple[index].equals(newValue))
                ) {
                    newTuple[index] = newValue;
                    updated = true;
                }
            }
            CommonId txnId = vertex.getTask().getTxnId();
            CommonId tableId = param.getTableId();
            CommonId partId = context.getDistribution().getId();
            KeyValueCodec codec = param.getCodec();
            StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
            if (context.getIndexId() != null) {
                Table indexTable = MetaService.root().getTable(context.getIndexId());
                List<Integer> columnIndices = param.getTable().getColumnIndices(indexTable.columns.stream()
                    .map(Column::getName)
                    .collect(Collectors.toList()));
                tableId = context.getIndexId();
                if (!param.isPessimisticTxn()) {
                    Object[] finalNewTuple = newTuple;
                    newTuple = columnIndices.stream().map(c -> finalNewTuple[c]).toArray();
                }
                schema = indexTable.tupleType();
                localStore = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
                codec = CodecService.getDefault().createKeyValueCodec(indexTable.tupleType(), indexTable.keyMapping());
            }
            StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
            Object[] newTuple2 = (Object[]) schema.convertFrom(newTuple, ValueConverter.INSTANCE);

            byte[] key = wrap(codec::encodeKey).apply(newTuple);
            CodecService.getDefault().setId(key, partId.domain);
            byte[] vectorKey;
            if (context.getIndexId() != null) {
                vectorKey = codec.encodeKeyPrefix(newTuple, 1);
                CodecService.getDefault().setId(vectorKey, partId.domain);
            } else {
                vectorKey = key;
            }
            byte[] primaryLockKey = param.getPrimaryLockKey();
            byte[] txnIdBytes = vertex.getTask().getTxnId().encode();
            byte[] tableIdBytes = tableId.encode();
            byte[] partIdBytes = partId.encode();
            if (param.isPessimisticTxn()) {
                byte[] jobIdByte = vertex.getTask().getJobId().encode();
                long forUpdateTs = vertex.getTask().getJobId().seq;
                byte[] forUpdateTsByte = PrimitiveCodec.encodeLong(forUpdateTs);
                int len = txnIdBytes.length + tableIdBytes.length + partIdBytes.length;
                // dataKeyValue   [10_txnId_tableId_partId_a_putIf, value]
                byte[] dataKey = ByteUtils.encode(
                    CommonId.CommonType.TXN_CACHE_DATA,
                    key,
                    Op.PUT.getCode(),
                    len,
                    txnIdBytes,
                    tableIdBytes,
                    partIdBytes
                );
                if (!updated) {
                    log.warn("{} updated is false key is {}", txnId, Arrays.toString(key));
                    byte[] rollBackKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey);
                    KeyValue rollBackKeyValue = new KeyValue(rollBackKey, null);
                    localStore.put(rollBackKeyValue);
                    return true;
                }
                KeyValue oldKeyValue = localStore.get(dataKey);
                byte[] primaryLockKeyBytes = (byte[]) ByteUtils.decodePessimisticExtraKey(primaryLockKey)[5];
                if (log.isDebugEnabled()) {
                    log.info("{} updated is true key is {}", txnId, Arrays.toString(key));
                }
                if (!(ByteArrayUtils.compare(key, primaryLockKeyBytes, 1) == 0)) {
                    // This key appears for the first time in the current transaction
                    if (oldKeyValue == null) {
                        KeyValue kvKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), vectorKey, param.getLockTimeOut());
                        if (kvKeyValue == null || kvKeyValue.getValue() == null) {
                            byte[] rollBackKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey);
                            KeyValue rollBackKeyValue = new KeyValue(rollBackKey, null);
                            localStore.put(rollBackKeyValue);
                            return true;
                        }
                        KeyValue keyValue = wrap(codec::encode).apply(newTuple2);
                        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
                        // write data
                        keyValue.setKey(dataKey);
                        if (localStore.put(keyValue)
                            && context.getIndexId() == null
                        ) {
                            param.inc();
                        }
                    } else {
                        KeyValue keyValue = wrap(codec::encode).apply(newTuple2);
                        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
                        // This key appears repeatedly in the current transaction
                        repeatKey(param, keyValue, txnId, localStore, dataKey,
                             context, updated);
                    }
                } else {
                    // primary lock not existed ：
                    // 1、first put primary lock
                    if (oldKeyValue == null) {
                        // first put primary lock
                        KeyValue kvKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), vectorKey, param.getLockTimeOut());
                        if (kvKeyValue == null || kvKeyValue.getValue() == null) {
                            byte[] rollBackKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey);
                            KeyValue rollBackKeyValue = new KeyValue(rollBackKey, null);
                            localStore.put(rollBackKeyValue);
                            return true;
                        }
                        KeyValue keyValue = wrap(codec::encode).apply(newTuple2);
                        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
                        // write data
                        keyValue.setKey(dataKey);
                        if (localStore.put(keyValue)
                            && context.getIndexId() == null
                        ) {
                            param.inc();
                        }
                    } else {
                        KeyValue keyValue = wrap(codec::encode).apply(newTuple2);
                        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
                        // primary lock existed ：
                        repeatKey(param, keyValue, txnId, localStore, dataKey,
                            context, updated);
                    }
                }
            } else {
                KeyValue keyValue = wrap(codec::encode).apply(newTuple2);
                CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
                keyValue.setKey(
                    ByteUtils.encode(
                        CommonId.CommonType.TXN_CACHE_DATA,
                        keyValue.getKey(),
                        Op.PUT.getCode(),
                        (txnIdBytes.length + tableIdBytes.length + partIdBytes.length),
                        txnIdBytes,
                        tableIdBytes,
                        partIdBytes)
                );
                byte[] insertKey = Arrays.copyOf(keyValue.getKey(), keyValue.getKey().length);
                insertKey[insertKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
                localStore.delete(insertKey);
                if (updated) {
                    localStore.delete(keyValue.getKey());
                    if (localStore.put(keyValue) && context.getIndexId() == null) {
                        param.inc();
                    }
                }
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
        return true;
    }

    private static void repeatKey(TxnPartUpdateParam param, KeyValue keyValue, CommonId txnId,
                                  StoreInstance store, byte[] dataKey, Context context, boolean updated) {
        // lock existed ：
        // 1、multi sql
        byte[] deleteKey = Arrays.copyOf(dataKey, dataKey.length);
        deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
        byte[] insertKey = Arrays.copyOf(dataKey, dataKey.length);
        insertKey[insertKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
        List<byte[]> bytes = new ArrayList<>(3);
        bytes.add(dataKey);
        bytes.add(deleteKey);
        bytes.add(insertKey);
        List<KeyValue> keyValues = store.get(bytes);
        if (keyValues != null && keyValues.size() > 0) {
            if (keyValues.size() > 1) {
                throw new RuntimeException(txnId + " PrimaryKey is not existed than two in local store");
            }
            // write data
            keyValue.setKey(dataKey);
            store.deletePrefix(deleteKey);
            store.deletePrefix(insertKey);
            if (updated) {
                store.deletePrefix(dataKey);
                if (store.put(keyValue) && context.getIndexId() == null) {
                    param.inc();
                }
            }
        } else {
            throw new RuntimeException(txnId + " PrimaryKey is not existed local store");
        }
    }
}
