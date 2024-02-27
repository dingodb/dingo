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
import io.dingodb.meta.entity.IndexTable;
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
import static io.dingodb.exec.utils.ByteUtils.decodePessimisticKey;

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
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
        KeyValueCodec codec = param.getCodec();
        boolean isVector = false;
        if (context.getIndexId() != null) {
            Table indexTable = MetaService.root().getTable(context.getIndexId());
            List<Integer> columnIndices = param.getTable().getColumnIndices(indexTable.columns.stream()
                .map(Column::getName)
                .collect(Collectors.toList()));
            tableId = context.getIndexId();
            IndexTable index = TransactionUtil.getIndexDefinitions(tableId);
            if (index.indexType.isVector) {
                isVector = true;
            }
            if (!param.isPessimisticTxn()) {
                Object[] finalTuple = tuple;
                tuple = columnIndices.stream().map(i -> finalTuple[i]).toArray();
            }
            localStore = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
            codec = CodecService.getDefault().createKeyValueCodec(indexTable.tupleType(), indexTable.keyMapping());
        }
        StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);

        byte[] keys = wrap(codec::encodeKey).apply(tuple);
        CodecService.getDefault().setId(keys, partId.domain);
        byte[] vectorKey;
        if (isVector) {
            vectorKey = codec.encodeKeyPrefix(tuple, 1);
            CodecService.getDefault().setId(vectorKey, partId.domain);
        } else {
            vectorKey = keys;
        }
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
            KeyValue oldKeyValue = localStore.get(dataKey);
            byte[] primaryLockKeyBytes = decodePessimisticKey(primaryLockKey);
            if (!(ByteArrayUtils.compare(keyValueKey, primaryLockKeyBytes, 1) == 0)) {
                // This key appears for the first time in the current transaction
                if (oldKeyValue == null) {
                    KeyValue kvKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), vectorKey, param.getLockTimeOut());
                    if (kvKeyValue == null || kvKeyValue.getValue() == null) {
                        return true;
                    }
                    byte[] rollBackKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey);
                    if (localStore.get(rollBackKey) != null) {
                        localStore.deletePrefix(rollBackKey);
                    }
                    KeyValue kv = wrap(codec::encode).apply(tuple);
                    CodecService.getDefault().setId(keys, partId.domain);
                    // write data
                    KeyValue dataKeyValue = new KeyValue(dataKey, kv.getValue());
                    if (localStore.put(dataKeyValue)
                        && context.getIndexId() == null
                    ) {
                        param.inc();
                    }
                } else {
                    // This key appears repeatedly in the current transaction
                    repeatKey(
                        param,
                        txnId,
                        localStore,
                        dataKey,
                        context
                    );
                }
            } else {
                // primary lock not existed ：
                // 1、first put primary lock
                if (oldKeyValue == null) {
                    byte[] rollBackKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey);
                    if (localStore.get(rollBackKey) != null) {
                        localStore.deletePrefix(rollBackKey);
                    }
                    KeyValue kvKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), vectorKey, param.getLockTimeOut());
                    if (kvKeyValue == null || kvKeyValue.getValue() == null) {
                        // first put primary lock
                        return true;
                    }
                    KeyValue kv = wrap(codec::encode).apply(tuple);
                    CodecService.getDefault().setId(keys, partId.domain);
                    // first put primary lock
                    // write data
                    KeyValue dataKeyValue = new KeyValue(dataKey, kv.getValue());
                    if (localStore.put(dataKeyValue)
                        && context.getIndexId() == null
                    ) {
                        param.inc();
                    }
                } else {
                    // primary lock existed ：
                    repeatKey(param,
                        txnId,
                        localStore,
                        dataKey,
                        context
                    );
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
            byte[] insertKey = Arrays.copyOf(keyValue.getKey(), keyValue.getKey().length);
            insertKey[insertKey.length - 2] = (byte) Op.PUT.getCode();
            localStore.deletePrefix(insertKey);
            insertKey[insertKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
            localStore.deletePrefix(insertKey);
            if (localStore.put(keyValue) && context.getIndexId() == null) {
                param.inc();
                context.addKeyState(true);
            }
        }

        return true;
    }

    private static void repeatKey(TxnPartDeleteParam param, CommonId txnId,
                                  StoreInstance store, byte[] dataKey, Context context) {
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
