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
import io.dingodb.common.CommonId;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Content;
import io.dingodb.exec.operator.params.TxnPartInsertParam;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class TxnPartInsertOperator extends PartModifyOperator {
    public static final TxnPartInsertOperator INSTANCE = new TxnPartInsertOperator();

    private TxnPartInsertOperator() {
    }

    @Override
    protected boolean pushTuple(Content content, Object[] tuple, Vertex vertex) {
        TxnPartInsertParam param = vertex.getParam();
        param.setContent(content);
        DingoType schema = param.getSchema();
        Object[] newTuple = (Object[]) schema.convertFrom(tuple, ValueConverter.INSTANCE);
        KeyValue keyValue = wrap(param.getCodec()::encode).apply(newTuple);
        CommonId txnId = vertex.getTask().getTxnId();
        CommonId tableId = param.getTableId();
        CommonId partId = content.getDistribution().getId();
        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
        byte[] primaryLockKey = param.getPrimaryLockKey();
        StoreInstance store = Services.LOCAL_STORE.getInstance(tableId, partId);
        byte[] txnIdByte = txnId.encode();
        byte[] tableIdByte = tableId.encode();
        byte[] partIdByte = partId.encode();
        if (param.isPessimisticTxn()) {
            byte[] keyValueKey = keyValue.getKey();
            byte[] jobIdByte = vertex.getTask().getJobId().encode();
            long forUpdateTs = vertex.getTask().getJobId().seq;
            byte[] forUpdateTsByte = PrimitiveCodec.encodeLong(forUpdateTs);
            int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
            // dataKeyValue   [10_txnId_tableId_partId_a_putIf, value]
            byte[] dataKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_DATA,
                keyValueKey,
                Op.PUTIFABSENT.getCode(),
                len,
                txnIdByte, tableIdByte, partIdByte);
            byte[] lockKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, dataKey);
            KeyValue oldKeyValue = store.get(lockKey);
            if (!(ByteArrayUtils.compare(keyValueKey, primaryLockKey, 9) == 0)) {
                // This key appears for the first time in the current transaction
                if (oldKeyValue == null) {
                    // for check deadLock
                    byte[] deadLockKeyBytes = ByteUtils.encode(
                        CommonId.CommonType.TXN_CACHE_BLOCK_LOCK,
                        keyValue.getKey(),
                        Op.LOCK.getCode(),
                        len,
                        txnIdByte,
                        tableIdByte,
                        partIdByte
                    );
                    KeyValue deadLockKeyValue = new KeyValue(deadLockKeyBytes, null);
                    store.put(deadLockKeyValue);
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
                    // get lock success, delete deadLockKey
                    store.deletePrefix(deadLockKeyBytes);
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
                            tableIdByte,
                            partIdByte),
                        keyValue.getValue()
                    );
                    // write data
                    keyValue.setKey(dataKey);
                    if (store.put(lockKeyValue) && store.put(extraKeyValue) && store.put(keyValue)) {
                        param.inc();
                    }
                } else {
                    // This key appears repeatedly in the current transaction
                    repeatKey(param, keyValue, txnId, keyValueKey, store, dataKey,
                        jobIdByte, tableIdByte, partIdByte, len);
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
                    repeatKey(param, keyValue, txnId, primaryLockKey, store, dataKey,
                        jobIdByte, tableIdByte, partIdByte, len);
                }
            }
        } else {
            keyValue.setKey(
                ByteUtils.encode(
                    CommonId.CommonType.TXN_CACHE_DATA,
                    keyValue.getKey(),
                    Op.PUTIFABSENT.getCode(),
                    (txnIdByte.length + tableIdByte.length + partIdByte.length),
                    txnIdByte,
                    tableIdByte,
                    partIdByte)
            );
            byte[] deleteKey = Arrays.copyOf(keyValue.getKey(), keyValue.getKey().length);
            deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
            store.delete(deleteKey);
            if (store.put(keyValue)) {
                param.inc();
            }
        }
        return true;
    }

    private static void repeatKey(TxnPartInsertParam param, KeyValue keyValue, CommonId txnId, byte[] key,
                                  StoreInstance store, byte[] dataKey, byte[] jobIdByte,
                                  byte[] tableIdByte, byte[] partIdByte, int len) {
        // lock existed ：
        // 1、multi sql
        // 2、signal sql: insert into values(1, 'a'),(1, 'b');
        byte[] deleteKey = Arrays.copyOf(dataKey, dataKey.length);
        deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
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
            byte[] oldKey = value.getKey();
            if (oldKey[oldKey.length - 2] == Op.PUTIFABSENT.getCode()
                || oldKey[oldKey.length - 2] == Op.PUT.getCode()) {
                throw new DuplicateEntryException("Duplicate entry " +
                    TransactionUtil.duplicateEntryKey(txnId, key) + "for key 'PRIMARY'");
            } else {
                // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
                byte[] extraKey = ByteUtils.encode(
                    CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                    key,
                    oldKey[oldKey.length - 2],
                    len,
                    jobIdByte,
                    tableIdByte,
                    partIdByte
                );
                KeyValue extraKeyValue = new KeyValue(extraKey, Arrays.copyOf(value.getValue(), value.getValue().length));
                // write data
                keyValue.setKey(dataKey);
                deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
                store.deletePrefix(deleteKey);
                if (store.put(extraKeyValue) && store.put(keyValue)) {
                    param.inc();
                }
            }
        } else {
            throw new RuntimeException(txnId + " PrimaryKey is not existed local store");
        }
    }

}
