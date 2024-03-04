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
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.PessimisticLockInsertParam;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.exec.utils.ByteUtils.decodePessimisticKey;
import static io.dingodb.exec.utils.ByteUtils.encode;
import static io.dingodb.exec.utils.ByteUtils.getKeyByOp;

@Slf4j
public class PessimisticLockInsertOperator extends SoleOutOperator {
    public static final PessimisticLockInsertOperator INSTANCE = new PessimisticLockInsertOperator();

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            PessimisticLockInsertParam param = vertex.getParam();
            param.setContext(context);
            CommonId txnId = vertex.getTask().getTxnId();
            CommonId tableId = param.getTableId();
            CommonId partId = context.getDistribution().getId();
            CommonId jobId = vertex.getTask().getJobId();
            byte[] primaryLockKey = param.getPrimaryLockKey();
            DingoType schema = param.getSchema();
            StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
            KeyValueCodec codec = param.getCodec();
            boolean isVector = false;
            if (context.getIndexId() != null) {
                Table indexTable = MetaService.root().getTable(context.getIndexId());
                List<Integer> columnIndices = param.getTable().getColumnIndices(indexTable.columns.stream()
                    .map(Column::getName)
                    .collect(Collectors.toList()));
                tableId = context.getIndexId();
                Object[] finalTuple = tuple;
                tuple = columnIndices.stream().map(i -> finalTuple[i]).toArray();
                schema = indexTable.tupleType();
                IndexTable index = TransactionUtil.getIndexDefinitions(tableId);
                if (index.indexType.isVector) {
                    isVector = true;
                }
                localStore = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
                codec = CodecService.getDefault().createKeyValueCodec(indexTable.tupleType(), indexTable.keyMapping());
            }
            StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
            Object[] newTuple = (Object[]) schema.convertFrom(tuple, ValueConverter.INSTANCE);
            KeyValue keyValue = wrap(codec::encode).apply(newTuple);
            CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
            byte[] key = keyValue.getKey();
            byte[] vectorKey;
            if (isVector) {
                vectorKey = codec.encodeKeyPrefix(newTuple, 1);
                CodecService.getDefault().setId(vectorKey, partId.domain);
            } else {
                vectorKey = key;
            }
            byte[] txnIdByte = txnId.encode();
            byte[] tableIdByte = tableId.encode();
            byte[] partIdByte = partId.encode();
            byte[] jobIdByte = vertex.getTask().getJobId().encode();
            int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
            byte[] lockKeyBytes = encode(
                CommonId.CommonType.TXN_CACHE_LOCK,
                key,
                Op.LOCK.getCode(),
                len,
                txnIdByte,
                tableIdByte,
                partIdByte
            );
            KeyValue oldKeyValue = localStore.get(lockKeyBytes);
            if (oldKeyValue == null) {
                // for check deadLock
                byte[] deadLockKeyBytes = encode(
                    CommonId.CommonType.TXN_CACHE_BLOCK_LOCK,
                    key,
                    Op.LOCK.getCode(),
                    len,
                    txnIdByte,
                    tableIdByte,
                    partIdByte
                );
                KeyValue deadLockKeyValue = new KeyValue(deadLockKeyBytes, null);
                localStore.put(deadLockKeyValue);

                byte[] primaryLockKeyBytes = decodePessimisticKey(primaryLockKey);
                long forUpdateTs = vertex.getTask().getJobId().seq;
                byte[] forUpdateTsByte = PrimitiveCodec.encodeLong(forUpdateTs);
                if (log.isDebugEnabled()) {
                    log.info("{}, forUpdateTs:{} txnPessimisticLock :{}", txnId, forUpdateTs, Arrays.toString(key));
                }
                TxnPessimisticLock txnPessimisticLock = TransactionUtil.pessimisticLock(
                    param.getLockTimeOut(),
                    txnId,
                    tableId,
                    partId,
                    primaryLockKeyBytes,
                    key,
                    param.getStartTs(),
                    forUpdateTs,
                    param.getIsolationLevel()
                );
                long newForUpdateTs = txnPessimisticLock.getForUpdateTs();
                if (newForUpdateTs != forUpdateTs) {
                    forUpdateTsByte = PrimitiveCodec.encodeLong(newForUpdateTs);
                }
                if (log.isDebugEnabled()) {
                    log.info("{}, forUpdateTs:{} txnPessimisticLock :{}", txnId, newForUpdateTs, Arrays.toString(key));
                }
                // get lock success, delete deadLockKey
                localStore.delete(deadLockKeyBytes);
                byte[] lockKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, deadLockKeyBytes);
                // lockKeyValue
                KeyValue lockKeyValue = new KeyValue(lockKey, forUpdateTsByte);
                localStore.put(lockKeyValue);
                // index use keyPrefix
//                KeyValue kvKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), vectorKey, param.getLockTimeOut());
//                if ((kvKeyValue == null || kvKeyValue.getValue() == null) && !param.isInsert()) {
//                    @Nullable Object[] finalTuple1 = tuple;
//                    vertex.getOutList().forEach(o -> o.transformToNext(context, finalTuple1));
//                    return true;
//                }
                // extraKeyValue
                KeyValue extraKeyValue = new KeyValue(
                    ByteUtils.encode(
                        CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                        key,
                        Op.NONE.getCode(),
                        len,
                        jobIdByte,
                        tableIdByte,
                        partIdByte),
                    keyValue.getValue()
                );
                localStore.put(extraKeyValue);
//                Object[] result = codec.decode(keyValue);
                vertex.getOutList().forEach(o -> o.transformToNext(context, newTuple));
                return true;
            } else {
                byte[] dataKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_DATA, Op.PUT, lockKeyBytes);
                byte[] deleteKey = Arrays.copyOf(dataKey, dataKey.length);
                deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
                byte[] updateKey = Arrays.copyOf(dataKey, dataKey.length);
                updateKey[updateKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
                List<byte[]> bytes = new ArrayList<>(3);
                bytes.add(dataKey);
                bytes.add(deleteKey);
                bytes.add(updateKey);
                List<KeyValue> keyValues = localStore.get(bytes);
                byte[] primaryLockKeyBytes = decodePessimisticKey(primaryLockKey);
                if (keyValues != null && keyValues.size() > 0) {
                    if (keyValues.size() > 1) {
                        throw new RuntimeException(txnId + " Key is not existed than two in local localStore");
                    }
                    KeyValue value = keyValues.get(0);
                    byte[] oldKey = value.getKey();
                    if (log.isDebugEnabled()) {
                        log.info("{}, repeat key :{}", txnId, Arrays.toString(oldKey));
                    }
                    if (oldKey[oldKey.length - 2] == Op.PUTIFABSENT.getCode()
                        || oldKey[oldKey.length - 2] == Op.PUT.getCode()) {
                        throw new DuplicateEntryException("Duplicate entry " +
                            TransactionUtil.duplicateEntryKey(tableId, key) + " for key 'PRIMARY'");
                    }
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
                    KeyValue extraKeyValue;
                    if (value.getValue() == null) {
                        // delete
                        extraKeyValue = new KeyValue(extraKey, null);
                    } else {
                        extraKeyValue = new KeyValue(extraKey, Arrays.copyOf(value.getValue(), value.getValue().length));
                    }
                    localStore.put(extraKeyValue);
//                    Object[] decode = decode(value);
//                    keyValue = new KeyValue(((TxnLocalData) decode[0]).getKey(), value.getValue());
//                    Object[] result = codec.decode(keyValue);
                    vertex.getOutList().forEach(o -> o.transformToNext(context, newTuple));
                    return true;
                } else {
                    KeyValue kvKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), vectorKey, param.getLockTimeOut());
                    if (kvKeyValue == null || kvKeyValue.getValue() == null) {
                        log.info("{}, repeat primary key :{} keyValue is null", txnId, Arrays.toString(primaryLockKeyBytes));
                        @Nullable Object[] finalTuple1 = tuple;
                        vertex.getOutList().forEach(o -> o.transformToNext(context, finalTuple1));
                        return true;
                    }
                    log.info("{}, repeat primary key :{} keyValue is not null", txnId, Arrays.toString(key));
                    if (isVector) {
                        kvKeyValue.setKey(codec.encodeKey(newTuple));
                    }
                    Object[] result = codec.decode(kvKeyValue);
                    vertex.getOutList().forEach(o -> o.transformToNext(context, result));
                    return true;
                }
            }
        }
    }

    @Override
    public synchronized void fin(int pin, Fin fin, Vertex vertex) {
        PessimisticLockInsertParam param = vertex.getParam();
        vertex.getSoleEdge().fin(fin);
        // Reset
        param.reset();
    }
}
