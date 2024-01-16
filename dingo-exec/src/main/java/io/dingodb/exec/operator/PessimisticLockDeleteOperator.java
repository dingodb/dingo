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
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.PessimisticLockDeleteParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import io.dingodb.store.api.transaction.exception.ReginSplitException;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.concurrent.Future;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class PessimisticLockDeleteOperator extends PartModifyOperator {
    public static final PessimisticLockDeleteOperator INSTANCE = new PessimisticLockDeleteOperator();

    @Override
    protected boolean pushTuple(@Nullable Object[] tuple, Vertex vertex) {
        PessimisticLockDeleteParam param = vertex.getParam();
        byte[] keys = wrap(param.getCodec()::encodeKey).apply(tuple);
        CommonId tableId = param.getTableId();
        CommonId jobId = vertex.getTask().getJobId();
        CommonId txnId = vertex.getTask().getTxnId();
        CommonId partId = PartitionService.getService(
                Optional.ofNullable(param.getTable().getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(keys, param.getDistributions());
        ITransaction transaction = TransactionManager.getTransaction(txnId);
        // add
        byte[] primaryKey = keys;
        long startTs = param.getStartTs();
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
            StoreInstance store = Services.KV_STORE.getInstance(tableId, partId);
            future = store.txnPessimisticLockPrimaryKey(txnPessimisticLock, param.getLockTimeOut());
        } catch (ReginSplitException e) {
            log.error(e.getMessage(), e);
            CommonId regionId = TransactionUtil.singleKeySplitRegionId(tableId, txnId, primaryKey);
            StoreInstance store = Services.KV_STORE.getInstance(tableId, regionId);
            future = store.txnPessimisticLockPrimaryKey(txnPessimisticLock, param.getLockTimeOut());
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            // primaryKeyLock rollback
            TransactionUtil.PessimisticPrimaryLockRollBack(
                txnId,
                tableId,
                partId,
                param.getIsolationLevel(),
                startTs,
                txnPessimisticLock.getForUpdateTs(),
                primaryKey
            );
            throw new RuntimeException(e.getMessage());
        }
        if (future == null) {
            // primaryKeyLock rollback
            TransactionUtil.PessimisticPrimaryLockRollBack(txnId, tableId, partId, param.getIsolationLevel(),
                startTs, txnPessimisticLock.getForUpdateTs(), primaryKey);
            throw new RuntimeException(txnId + " future is null " + partId + ",txnPessimisticLockPrimaryKey false");
        }
        long forUpdateTs = txnPessimisticLock.getForUpdateTs();
        transaction.setForUpdateTs(forUpdateTs);
        transaction.setPrimaryKeyFuture(future);
        StoreInstance store = Services.LOCAL_STORE.getInstance(tableId, partId);
        byte[] jobIdByte = jobId.encode();
        byte[] txnIdByte = txnId.encode();
        byte[] tableIdByte = tableId.encode();
        byte[] partIdByte = partId.encode();
        byte[] forUpdateTsByte = PrimitiveCodec.encodeLong(forUpdateTs);
        int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
        // lockKeyValue  [11_txnId_tableId_partId_a_lock, forUpdateTs1]
        transaction.setPrimaryKeyLock(
            ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_LOCK,
                keys,
                Op.LOCK.getCode(),
                len,
                txnIdByte,
                tableIdByte,
                partIdByte)
        );
        // extraKeyValue  [12_jobId_tableId_partId_a_none, value]
        byte[] extraKeyBytes = ByteUtils.encode(
            CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
            keys,
            Op.NONE.getCode(),
            len,
            jobIdByte,
            tableIdByte,
            partIdByte
        );
        KeyValue extraKeyValue = new KeyValue(extraKeyBytes, null);
        // dataKeyValue   [10_txnId_tableId_partId_a_delete, value]
        KeyValue dataKeyValue = new KeyValue(
            ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_DATA,
                keys,
                Op.DELETE.getCode(),
                len,
                txnIdByte,
                tableIdByte,
                partIdByte), null);
        if (store.put(extraKeyValue) && store.put(dataKeyValue)) {
            param.inc();
        }
        return false;
    }
}
