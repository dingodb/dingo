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
import io.dingodb.exec.operator.params.PessimisticLockParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.exec.utils.ByteUtils.encode;
import static io.dingodb.exec.utils.ByteUtils.getKeyByOp;

@Slf4j
public class PessimisticLockOperator extends SoleOutOperator {
    public static final PessimisticLockOperator INSTANCE = new PessimisticLockOperator();

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
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
            boolean isVector = false;
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
                IndexTable index = TransactionUtil.getIndexDefinitions(tableId);
                if (index.indexType.isVector) {
                    isVector = true;
                }
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
            byte[] key = wrap(codec::encodeKey).apply(newTuple);
            CodecService.getDefault().setId(key, partId.domain);
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
                // first
                // add
                byte[] primaryKey = Arrays.copyOf(key, key.length);
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
                    TransactionUtil.resolvePessimisticLock(
                        param.getIsolationLevel(),
                        txnId,
                        tableId,
                        partId,
                        deadLockKeyBytes,
                        primaryKey,
                        startTs,
                        txnPessimisticLock.getForUpdateTs(),
                        true,
                        e
                    );
                }
                if (future == null) {
                    TransactionUtil.resolvePessimisticLock(
                        param.getIsolationLevel(),
                        txnId,
                        tableId,
                        partId,
                        deadLockKeyBytes,
                        primaryKey,
                        startTs,
                        txnPessimisticLock.getForUpdateTs(),
                        true,
                        new RuntimeException(txnId + " future is null " + partId + ",txnPessimisticLockPrimaryKey false")
                    );
                }
                KeyValue kvKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), vectorKey, param.getLockTimeOut());
                if (param.isInsert()) {
                    if (kvKeyValue != null && kvKeyValue.getValue() != null) {
                        if (future != null) {
                            future.cancel(true);
                        }
                        TransactionUtil.resolvePessimisticLock(
                            param.getIsolationLevel(),
                            txnId,
                            tableId,
                            partId,
                            deadLockKeyBytes,
                            primaryKey,
                            startTs,
                            txnPessimisticLock.getForUpdateTs(),
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
                localStore.delete(deadLockKeyBytes);
                // lockKeyValue  [11_txnId_tableId_partId_a_lock, forUpdateTs1]
                byte[] primaryKeyLock = getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, deadLockKeyBytes);
                transaction.setPrimaryKeyLock(primaryKeyLock);
                if (!Arrays.equals(transaction.getPrimaryKeyLock(), primaryKeyLock)) {
                    if (future != null) {
                        future.cancel(true);
                    }
                    TransactionUtil.resolvePessimisticLock(
                        param.getIsolationLevel(),
                        txnId,
                        tableId,
                        partId,
                        deadLockKeyBytes,
                        primaryKey,
                        startTs,
                        txnPessimisticLock.getForUpdateTs(),
                        false,
                        null
                    );
                    return false;
                }
                transaction.setForUpdateTs(forUpdateTs);
                transaction.setPrimaryKeyFuture(future);
                if (kvKeyValue != null && kvKeyValue.getValue() != null) {
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
                        kvKeyValue.getValue()
                    );
                    localStore.put(extraKeyValue);
                } else {
                    if (param.isInsert()) {
                        KeyValue keyValue = wrap(codec::encode).apply(newTuple);
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
                    } else {
                        byte[] rollBackKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, deadLockKeyBytes);
                        localStore.put(new KeyValue(rollBackKey, null));
                    }
                }
                byte[] lockKey = getKeyByOp(CommonId.CommonType.TXN_CACHE_LOCK, Op.LOCK, deadLockKeyBytes);
                // lockKeyValue
                KeyValue lockKeyValue = new KeyValue(lockKey, PrimitiveCodec.encodeLong(forUpdateTs));
                localStore.put(lockKeyValue);
                return false;
            } else {
                log.warn("{}, key exist in localStore :{} ", txnId, Arrays.toString(key));
            }
            return true;
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
