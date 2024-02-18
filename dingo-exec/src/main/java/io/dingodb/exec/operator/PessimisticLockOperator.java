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
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.PartModifyParam;
import io.dingodb.exec.operator.params.PessimisticLockInsertParam;
import io.dingodb.exec.operator.params.PessimisticLockParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.exec.utils.ByteUtils.*;

@Slf4j
public class PessimisticLockOperator extends SoleOutOperator {
    public static final PessimisticLockOperator INSTANCE = new PessimisticLockOperator();

    @Override
    public synchronized boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        PessimisticLockParam param = vertex.getParam();
        param.setContext(context);
        CommonId txnId = vertex.getTask().getTxnId();
        CommonId tableId = param.getTableId();
        CommonId partId = context.getDistribution().getId();
        CommonId jobId = vertex.getTask().getJobId();
        byte[] primaryLockKey = param.getPrimaryLockKey();
        ITransaction transaction = TransactionManager.getTransaction(txnId);
        if (transaction == null || (primaryLockKey == null && transaction.getPrimaryKeyLock() != null)) {
            return false;
        }
        DingoType schema = param.getSchema();
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
        KeyValueCodec codec = param.getCodec();
        if (context.getIndexId() != null) {
            if (primaryLockKey == null) {
                return true;
            }
            Table indexTable = MetaService.root().getTable(context.getIndexId());
            List<Integer> columnIndices = param.getTable().getColumnIndices(indexTable.columns.stream()
                .map(Column::getName)
                .collect(Collectors.toList()));
            tableId = context.getIndexId();
            Object[] finalTuple = tuple;
            tuple = columnIndices.stream().map(i -> finalTuple[i]).toArray();
            schema = indexTable.tupleType();
            localStore = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
            codec = CodecService.getDefault().createKeyValueCodec(indexTable.tupleType(), indexTable.keyMapping());
        }
        StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
        Object[] newTuple;
        if (schema.fieldCount() != tuple.length) {
            Object[] dest = new Object[schema.fieldCount()];
            System.arraycopy(tuple, 0, dest, 0, schema.fieldCount());
            newTuple = (Object[]) schema.convertFrom(dest, ValueConverter.INSTANCE);
        } else {
            newTuple = (Object[]) schema.convertFrom(tuple, ValueConverter.INSTANCE);
        }
        KeyValue keyValue = wrap(codec::encode).apply(newTuple);
        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
        byte[] txnIdByte = txnId.encode();
        byte[] tableIdByte = tableId.encode();
        byte[] partIdByte = partId.encode();
        byte[] jobIdByte = vertex.getTask().getJobId().encode();
        int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
        byte[] lockKeyBytes = encode(
            CommonId.CommonType.TXN_CACHE_LOCK,
            keyValue.getKey(),
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
                keyValue.getKey(),
                Op.LOCK.getCode(),
                len,
                txnIdByte,
                tableIdByte,
                partIdByte
            );
            KeyValue deadLockKeyValue = new KeyValue(deadLockKeyBytes, null);
            localStore.put(deadLockKeyValue);
            // first
            if (primaryLockKey == null) {
                // add
                byte[] primaryKey = Arrays.copyOf(keyValue.getKey(), keyValue.getKey().length);
                long startTs = param.getStartTs();
                if (log.isDebugEnabled()) {
                    log.info("{}, forUpdateTs:{} txnPessimisticLock :{}", txnId, jobId.seq, Arrays.toString(primaryKey));
                }
                Future future = null;
                TxnPessimisticLock txnPessimisticLock = TxnPessimisticLock.builder().
                    isolationLevel(IsolationLevel.of(param.getIsolationLevel()))
                    .primaryLock(primaryKey)
                    .mutations(Collections.singletonList(
                        TransactionCacheToMutation.cacheToPessimisticLockMutation(
                            primaryKey, TransactionUtil.toLockExtraData(
                                tableId,
                                partId,
                                txnId,
                                TransactionType.PESSIMISTIC.getCode()
                            ), jobId.seq
                        )
                    ))
                    .lockTtl(TransactionManager.lockTtlTm())
                    .startTs(startTs)
                    .forUpdateTs(jobId.seq)
                    .build();
                try {
                    future = kvStore.txnPessimisticLockPrimaryKey(txnPessimisticLock, param.getLockTimeOut());
                } catch (RegionSplitException e) {
                    log.error(e.getMessage(), e);
                    CommonId regionId = TransactionUtil.singleKeySplitRegionId(tableId, txnId, primaryKey);
                    kvStore = Services.KV_STORE.getInstance(tableId, regionId);
                    future = kvStore.txnPessimisticLockPrimaryKey(txnPessimisticLock, param.getLockTimeOut());
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                    resolvePessimisticLock(
                        param,
                        txnId,
                        tableId,
                        partId,
                        deadLockKeyBytes,
                        primaryKey,
                        startTs,
                        txnPessimisticLock,
                        true,
                        e
                    );
                }
                if (future == null) {
                    resolvePessimisticLock(
                        param,
                        txnId,
                        tableId,
                        partId,
                        deadLockKeyBytes,
                        primaryKey,
                        startTs,
                        txnPessimisticLock,
                        true,
                        new RuntimeException(txnId + " future is null " + partId + ",txnPessimisticLockPrimaryKey false")
                    );
                }
                if (param.isInsert()) {
                    KeyValue kvKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), primaryKey, param.getLockTimeOut());
                    if (kvKeyValue != null && kvKeyValue.getValue() != null) {
                        if (future != null) {
                            future.cancel(true);
                        }
                        resolvePessimisticLock(
                            param,
                            txnId,
                            tableId,
                            partId,
                            deadLockKeyBytes,
                            primaryKey,
                            startTs,
                            txnPessimisticLock,
                            true,
                            new DuplicateEntryException("Duplicate entry " +
                                TransactionUtil.duplicateEntryKey(CommonId.decode(tableIdByte), primaryKey) + " for key 'PRIMARY'")
                        );
                    }
                }
                long forUpdateTs = txnPessimisticLock.getForUpdateTs();
                if (log.isDebugEnabled()) {
                    log.info("{}, forUpdateTs:{} txnPessimisticLock :{} end", txnId, forUpdateTs, Arrays.toString(primaryKey));
                }
                // get lock success, delete deadLockKey
                localStore.deletePrefix(deadLockKeyBytes);
                // lockKeyValue  [11_txnId_tableId_partId_a_lock, forUpdateTs1]
                byte[] primaryKeyLock = getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, deadLockKeyBytes);
                transaction.setPrimaryKeyLock(primaryKeyLock);
                if (!Arrays.equals(transaction.getPrimaryKeyLock(), primaryKeyLock)) {
                    if (future != null) {
                        future.cancel(true);
                    }
                    resolvePessimisticLock(
                        param,
                        txnId,
                        tableId,
                        partId,
                        deadLockKeyBytes,
                        primaryKey,
                        startTs,
                        txnPessimisticLock,
                        false,
                        null
                    );
                    return false;
                }
                transaction.setForUpdateTs(forUpdateTs);
                transaction.setPrimaryKeyFuture(future);
                KeyValue newKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), keyValue.getKey(), param.getLockTimeOut());
                byte[] value;
                if (newKeyValue != null && newKeyValue.getValue() != null) {
                    value = newKeyValue.getValue();
                } else {
                    value = keyValue.getValue();
                }
                byte[] lockKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, deadLockKeyBytes);
                // lockKeyValue
                KeyValue lockKeyValue = new KeyValue(lockKey, PrimitiveCodec.encodeLong(forUpdateTs));
                // extraKeyValue
                KeyValue extraKeyValue = new KeyValue(
                    ByteUtils.encode(
                        CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                        keyValue.getKey(),
                        Op.NONE.getCode(),
                        len,
                        jobIdByte,
                        tableIdByte,
                        partIdByte),
                    value
                );
                localStore.put(lockKeyValue);
                localStore.put(extraKeyValue);
                return false;
            } else {
                byte[] primaryLockKeyBytes = (byte[]) decodePessimisticExtraKey(primaryLockKey)[5];
                long forUpdateTs = vertex.getTask().getJobId().seq;
                byte[] forUpdateTsByte = PrimitiveCodec.encodeLong(forUpdateTs);
                if (log.isDebugEnabled()) {
                    log.info("{}, forUpdateTs:{} txnPessimisticLock :{}", txnId, forUpdateTs, Arrays.toString(keyValue.getKey()));
                }
                TxnPessimisticLock txnPessimisticLock = TransactionUtil.pessimisticLock(
                    param.getLockTimeOut(),
                    txnId,
                    tableId,
                    partId,
                    primaryLockKeyBytes,
                    keyValue.getKey(),
                    param.getStartTs(),
                    forUpdateTs,
                    param.getIsolationLevel()
                );
                long newForUpdateTs = txnPessimisticLock.getForUpdateTs();
                if (newForUpdateTs != forUpdateTs) {
                    forUpdateTsByte = PrimitiveCodec.encodeLong(newForUpdateTs);
                }
                if (log.isDebugEnabled()) {
                    log.info("{}, forUpdateTs:{} txnPessimisticLock :{}", txnId, newForUpdateTs, Arrays.toString(keyValue.getKey()));
                }
                // get lock success, delete deadLockKey
                localStore.delete(deadLockKeyBytes);
                byte[] lockKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, deadLockKeyBytes);
                // lockKeyValue
                KeyValue lockKeyValue = new KeyValue(lockKey, forUpdateTsByte);
                // extraKeyValue
                KeyValue extraKeyValue = new KeyValue(
                    ByteUtils.encode(
                        CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                        keyValue.getKey(),
                        Op.NONE.getCode(),
                        len,
                        jobIdByte,
                        tableIdByte,
                        partIdByte),
                    keyValue.getValue()
                );
                localStore.put(lockKeyValue);
                localStore.put(extraKeyValue);
                keyValue = kvStore.txnGet(TsoService.getDefault().tso(), keyValue.getKey(), param.getLockTimeOut());
                if (keyValue == null || keyValue.getValue() == null) {
                    @Nullable Object[] finalTuple1 = tuple;
                    vertex.getOutList().forEach(o -> o.transformToNext(context, finalTuple1));
                    return true;
                }
                Object[] result = param.getCodec().decode(keyValue);
                vertex.getOutList().forEach(o -> o.transformToNext(context, result));
                return true;
            }

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
            byte[] primaryLockKeyBytes = (byte[]) ByteUtils.decodePessimisticExtraKey(primaryLockKey)[5];
            if (keyValues != null && keyValues.size() > 0) {
                if (keyValues.size() > 1) {
                    throw new RuntimeException(txnId + " Key is not existed than two in local localStore");
                }
                KeyValue value = keyValues.get(0);
                byte[] oldKey = value.getKey();
                if (log.isDebugEnabled()) {
                    log.info("{}, repeat key :{}", txnId, Arrays.toString(oldKey));
                }
                // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
                byte[] extraKey = ByteUtils.encode(
                    CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                    keyValue.getKey(),
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
                Object[] decode = decode(value);
                keyValue = new KeyValue((byte[]) decode[5], value.getValue());
                Object[] result = param.getCodec().decode(keyValue);
                vertex.getOutList().forEach(o -> o.transformToNext(context, result));
                return true;
            } else {
                keyValue = kvStore.txnGet(TsoService.getDefault().tso(), keyValue.getKey(), param.getLockTimeOut());
                if (keyValue == null || keyValue.getValue() == null) {
                    if (log.isDebugEnabled()) {
                        log.info("{}, repeat primary key :{} keyValue is null", txnId, Arrays.toString(primaryLockKeyBytes));
                    }
                    @Nullable Object[] finalTuple1 = tuple;
                    vertex.getOutList().forEach(o -> o.transformToNext(context, finalTuple1));
                    return true;
                }
                if (log.isDebugEnabled()) {
                    log.info("{}, repeat primary key :{} keyValue is not null", txnId, Arrays.toString(keyValue.getKey()));
                }
                Object[] result = param.getCodec().decode(keyValue);
                vertex.getOutList().forEach(o -> o.transformToNext(context, result));
                return true;
            }
        }
    }

    private void resolvePessimisticLock(PessimisticLockParam param, CommonId txnId, CommonId tableId,
                                        CommonId partId, byte[] deadLockKeyBytes, byte[] primaryKey,
                                        long startTs, TxnPessimisticLock txnPessimisticLock,
                                        boolean hasException, Throwable e) {
        StoreInstance store;
        // primaryKeyLock rollback
        TransactionUtil.pessimisticPrimaryLockRollBack(
            txnId,
            tableId,
            partId,
            param.getIsolationLevel(),
            startTs,
            txnPessimisticLock.getForUpdateTs(),
            primaryKey
        );
        store = Services.LOCAL_STORE.getInstance(tableId, partId);
        // delete deadLockKey
        store.deletePrefix(deadLockKeyBytes);
        if (hasException){
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public synchronized void fin(int pin, Fin fin, Vertex vertex) {
        PessimisticLockParam param = vertex.getParam();
        vertex.getSoleEdge().fin(fin);
        // Reset
        param.reset();
    }
}
