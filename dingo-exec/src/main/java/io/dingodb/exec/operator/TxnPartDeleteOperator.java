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
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnPartDeleteParam;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class TxnPartDeleteOperator extends PartModifyOperator {
    public final static TxnPartDeleteOperator INSTANCE = new TxnPartDeleteOperator();

    private TxnPartDeleteOperator() {
    }

    @Override
    protected boolean pushTuple(Context context, Object[] tuple, Vertex vertex) {
        TxnPartDeleteParam param = vertex.getParam();
        param.setContext(context);
        CommonId txnId = vertex.getTask().getTxnId();
        CommonId tableId = param.getTableId();
        CommonId partId = context.getDistribution().getId();
        StoreInstance store = Services.LOCAL_STORE.getInstance(tableId, partId);
        KeyValueCodec codec = param.getCodec();
        if (context.getIndexId() != null) {
            Table indexTable = MetaService.root().getTable(context.getIndexId());
            List<Integer> columnIndices = param.getTable().getColumnIndices(indexTable.columns.stream()
                .map(Column::getName)
                .collect(Collectors.toList()));
            tableId = context.getIndexId();
            Object[] finalTuple = tuple;
            tuple = columnIndices.stream().map(i -> finalTuple[i]).toArray();
            store = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
            codec = CodecService.getDefault().createKeyValueCodec(indexTable.tupleType(), indexTable.keyMapping());
        }
        KeyValue kv = wrap(codec::encode).apply(tuple);
        byte[] keys = kv.getKey();
        CodecService.getDefault().setId(keys, partId.domain);
        byte[] primaryLockKey = param.getPrimaryLockKey();
        byte[] txnIdBytes = vertex.getTask().getTxnId().encode();
        byte[] tableIdBytes = tableId.encode();
        byte[] partIdBytes = partId.encode();
        if (param.isPessimisticTxn()) {
            byte[] keyValueKey = keys;
            byte[] jobIdByte = vertex.getTask().getJobId().encode();
            long forUpdateTs = vertex.getTask().getJobId().seq;
            byte[] forUpdateTsByte = PrimitiveCodec.encodeLong(forUpdateTs);
            int len = txnIdBytes.length + tableIdBytes.length + partIdBytes.length;
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
            KeyValue oldKeyValue = store.get(dataKey);
            byte[] primaryLockKeyBytes = (byte[]) ByteUtils.decodePessimisticExtraKey(primaryLockKey)[5];
            if (!(ByteArrayUtils.compare(keyValueKey, primaryLockKeyBytes, 1) == 0)) {
                // This key appears for the first time in the current transaction
                if (oldKeyValue == null) {
                    byte[] rollBackKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey);
                    store = Services.LOCAL_STORE.getInstance(tableId, partId);
                    if (store.get(rollBackKey) != null) {
                        store.deletePrefix(rollBackKey);
                    }
                    // write data
                    KeyValue dataKeyValue = new KeyValue(dataKey, kv.getValue());
                    if (store.put(dataKeyValue)
                        && context.getIndexId() == null
                    ) {
                        param.inc();
                    }
                } else {
                    // This key appears repeatedly in the current transaction
                    repeatKey(
                        param,
                        txnId,
                        keyValueKey,
                        store,
                        dataKey,
                        jobIdByte,
                        tableIdBytes,
                        partIdBytes,
                        len,
                        context);
                }
            } else {
                // primary lock not existed ：
                // 1、first put primary lock
                if (oldKeyValue == null) {
                    byte[] rollBackKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey);
                    store = Services.LOCAL_STORE.getInstance(tableId, partId);
                    if (store.get(rollBackKey) != null) {
                        store.deletePrefix(rollBackKey);
                    } else {
                        StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
                        KeyValue kvKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), keyValueKey, param.getLockTimeOut());
                        if (kvKeyValue == null || kvKeyValue.getValue() == null) {
                            // first put primary lock
                            // write data
                            KeyValue dataKeyValue = new KeyValue(dataKey, kv.getValue());
                            store.put(dataKeyValue);
                            return true;
                        }
                    }
                    // first put primary lock
                    // write data
                    KeyValue dataKeyValue = new KeyValue(dataKey, kv.getValue());
                    if (store.put(dataKeyValue)
                        && context.getIndexId() == null
                    ) {
                        param.inc();
                    }
                } else {
                    // primary lock existed ：
                    repeatKey(param,
                        txnId,
                        primaryLockKeyBytes,
                        store,
                        dataKey,
                        jobIdByte,
                        tableIdBytes,
                        partIdBytes,
                        len,
                        context);
                }
            }
        } else {
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
            byte[] insertKey = Arrays.copyOf(keyValue.getKey(), keyValue.getKey().length);
            insertKey[insertKey.length - 2] = (byte) Op.PUT.getCode();
            store.deletePrefix(insertKey);
            insertKey[insertKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
            store.deletePrefix(insertKey);
            if (store.put(keyValue) && context.getIndexId() == null) {
                param.inc();
            }
        }

        return true;
    }

    private static void repeatKey(TxnPartDeleteParam param, CommonId txnId, byte[] key,
                                  StoreInstance store, byte[] dataKey, byte[] jobIdByte,
                                  byte[] tableIdByte, byte[] partIdByte, int len, Context context) {
        // lock existed ：
        // 1、multi sql
        byte[] insertKey = Arrays.copyOf(dataKey, dataKey.length);
        insertKey[insertKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
        byte[] updateKey = Arrays.copyOf(dataKey, dataKey.length);
        updateKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
        List<byte[]> bytes = new ArrayList<>(3);
        bytes.add(dataKey);
        bytes.add(insertKey);
        bytes.add(updateKey);
        List<KeyValue> keyValues = store.get(bytes);
        if (keyValues != null && keyValues.size() > 0) {
            if (keyValues.size() > 1) {
                throw new RuntimeException(txnId + " PrimaryKey is not existed than two in local store");
            }
            // write data
            KeyValue dataKeyValue = new KeyValue(dataKey, null);
            store.deletePrefix(insertKey);
            store.deletePrefix(updateKey);
            if (store.put(dataKeyValue) && context.getIndexId() == null) {
                param.inc();
            }
        } else {
            throw new RuntimeException(txnId + " PrimaryKey is not existed local store");
        }
    }

}
