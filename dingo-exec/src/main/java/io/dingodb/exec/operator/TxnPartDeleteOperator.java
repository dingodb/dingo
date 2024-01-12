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

import io.dingodb.common.CommonId;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.TxnPartDeleteParam;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

public class TxnPartDeleteOperator extends PartModifyOperator {
    public final static TxnPartDeleteOperator INSTANCE = new TxnPartDeleteOperator();

    private TxnPartDeleteOperator() {
    }

    @Override
    protected boolean pushTuple(Object[] tuple, Vertex vertex) {
        TxnPartDeleteParam param = vertex.getParam();
        byte[] keys = wrap(param.getCodec()::encodeKey).apply(tuple);
        CommonId txnId = vertex.getTask().getTxnId();
        CommonId tableId = param.getTableId();
        CommonId partId = PartitionService.getService(
                Optional.ofNullable(param.getTableDefinition().getPartDefinition())
                    .map(PartitionDefinition::getFuncName)
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(keys, param.getDistributions());
        byte[] primaryLockKey = param.getPrimaryLockKey();
        StoreInstance store = Services.LOCAL_STORE.getInstance(tableId, partId);
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
            byte[] primaryLockKeyBytes = (byte[]) ByteUtils.decodePessimisticExtraKey(primaryLockKey)[5];
            byte[] lockKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, dataKey);
            KeyValue oldKeyValue = store.get(lockKey);
            if (!(ByteArrayUtils.compare(keyValueKey, primaryLockKeyBytes, 9) == 0)) {
                // This key appears for the first time in the current transaction
                if (oldKeyValue == null) {
                    TxnPessimisticLock txnPessimisticLock = TransactionUtil.pessimisticLock(
                        param.getLockTimeOut(),
                        txnId,
                        tableId,
                        partId,
                        primaryLockKey,
                        keyValueKey,
                        param.getStartTs(),
                        forUpdateTs,
                        param.getIsolationLevel()
                    );
                    long newForUpdateTs = txnPessimisticLock.getForUpdateTs();
                    if (newForUpdateTs != forUpdateTs) {
                        forUpdateTsByte = PrimitiveCodec.encodeLong(newForUpdateTs);
                    }
                    // lockKeyValue
                    KeyValue lockKeyValue = new KeyValue(lockKey, forUpdateTsByte);
                    // extraKeyValue
                    KeyValue extraKeyValue = new KeyValue(
                        ByteUtils.encode(
                            CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                            keyValueKey,
                            Op.NONE.getCode(),
                            len,
                            jobIdByte,
                            tableIdBytes,
                            partIdBytes),
                        null);
                    // write data
                    KeyValue dataKeyValue = new KeyValue(dataKey, null);
                    if (store.put(lockKeyValue) && store.put(extraKeyValue) && store.put(dataKeyValue)) {
                        param.inc();
                    }
                } else {
                    // This key appears repeatedly in the current transaction
                    repeatKey(param, txnId, keyValueKey, store, dataKey, jobIdByte, tableIdBytes, partIdBytes, len);
                }
            } else {
                // primary lock not existed ：
                // 1、first put primary lock
                if (oldKeyValue == null) {
                    // first put primary lock
                    // put lock data
                    KeyValue lockKeyValue = new KeyValue(lockKey, forUpdateTsByte);
                    if (store.put(lockKeyValue)) {
                        param.inc();
                    }
                } else {
                    // primary lock existed ：
                    repeatKey(param, txnId, primaryLockKey, store, dataKey, jobIdByte, tableIdBytes, partIdBytes, len);
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
            KeyValue keyValue = new KeyValue(resultKeys, null);
            byte[] insertKey = Arrays.copyOf(keyValue.getKey(), keyValue.getKey().length);
            insertKey[insertKey.length - 2] = (byte) Op.PUT.getCode();
            store.deletePrefix(insertKey);
            insertKey[insertKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
            store.deletePrefix(insertKey);
            if (store.put(keyValue)) {
                param.inc();
            }
        }

        return true;
    }

    private static void repeatKey(TxnPartDeleteParam param, CommonId txnId, byte[] key,
                                  StoreInstance store, byte[] dataKey, byte[] jobIdByte,
                                  byte[] tableIdByte, byte[] partIdByte, int len) {
        // lock existed ：
        // 1、multi sql
        byte[] deleteKey = Arrays.copyOf(dataKey, dataKey.length);
        deleteKey[deleteKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
        byte[] updateKey = Arrays.copyOf(dataKey, dataKey.length);
        updateKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
        List<byte[]> bytes = new ArrayList<>(3);
        bytes.add(dataKey);
        bytes.add(deleteKey);
        bytes.add(updateKey);
        List<KeyValue> keyValues = store.get(bytes);
        if (keyValues != null && keyValues.size() > 0) {
            if (keyValues.size() > 1) {
                throw new RuntimeException(txnId + " PrimaryKey is not existed than two in local store");
            }
            KeyValue value = keyValues.get(0);
            // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
            byte[] extraKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                key,
                value.getKey()[value.getKey().length - 2],
                len,
                jobIdByte,
                tableIdByte,
                partIdByte
            );
            KeyValue extraKeyValue;
            if (value.getValue() == null) {
                 // delete
                extraKeyValue = new KeyValue(extraKey, null);
            } else {
                extraKeyValue = new KeyValue(extraKey, Arrays.copyOf(value.getValue(), value.getValue().length));
            }
            // write data
            KeyValue dataKeyValue = new KeyValue(dataKey, null);
            store.deletePrefix(deleteKey);
            store.deletePrefix(updateKey);
            if (store.put(extraKeyValue) && store.put(dataKeyValue)) {
                param.inc();
            }
        } else {
            throw new RuntimeException(txnId + " PrimaryKey is not existed local store");
        }
    }

}
