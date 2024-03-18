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

package io.dingodb.exec.transaction.impl;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.transaction.base.BaseTransaction;
import io.dingodb.exec.transaction.base.CacheToObject;
import io.dingodb.exec.transaction.base.TransactionStatus;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.transaction.visitor.DingoTransactionRenderJob;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Future;

@Slf4j
public class OptimisticTransaction extends BaseTransaction {
    public OptimisticTransaction(long startTs, int isolationLevel) {
        super(startTs, isolationLevel);
    }

    public OptimisticTransaction(CommonId txnId, int isolationLevel) {
        super(txnId, isolationLevel);
    }

    @Override
    public TransactionType getType() {
        return TransactionType.OPTIMISTIC;
    }

    @Override
    public void rollBackPessimisticLock(JobManager jobManager) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollBackPessimisticPrimaryLock(JobManager jobManager) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getForUpdateTs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setForUpdateTs(long forUpdateTs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getPrimaryKeyLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPrimaryKeyLock(byte[] primaryKeyLock) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPrimaryKeyFuture(Future future) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollBackResidualPessimisticLock(JobManager jobManager) {
        throw new UnsupportedOperationException();
    }

    public void retryPrepare() {
//        TransactionManager.unregister(txnId);
        long start_ts = TransactionManager.nextTimestamp();
        this.startTs = start_ts;
//        this.txnInstanceId = new CommonId(CommonId.CommonType.TXN_INSTANCE, start_ts, 0l);
//        this.txnId = new CommonId(CommonId.CommonType.TRANSACTION, TransactionManager.getServerId().seq, start_ts);
//        TransactionManager.register(txnId, this);
        this.status = TransactionStatus.PRE_WRITE_RETRY;
    }

    public Job createRetryJob(JobManager jobManager) {
        long jobSeqId = TransactionManager.nextTimestamp();
        Job job = jobManager.createJob(startTs, jobSeqId, txnId, null);
        return job;
    }
    public void retryRun(JobManager jobManager, Job job, Location currentLocation) {
        DingoTransactionRenderJob.renderPreWriteJob(job, currentLocation, this, true);
        Iterator<Object[]> iterator = jobManager.createIterator(job, null);
        while (iterator.hasNext()) {
            Object[] next = iterator.next();
        }
    }

    @Override
    public void preWritePrimaryKey() {
        // 1、get first key from cache
        cacheToObject = cache.getPrimaryKey();
        byte[] key = cacheToObject.getMutation().getKey();
        primaryKey = key;
        txnPreWritePrimaryKey(cacheToObject);
    }

    private void txnPreWritePrimaryKey(CacheToObject cacheToObject) {
        Future future = null;
        Integer retry = Optional.mapOrGet(DingoConfiguration.instance().find("retry", int.class), __ -> __, () -> 30);
        while (retry-- > 0) {
            // 2、call sdk preWritePrimaryKey
            TxnPreWrite txnPreWrite = TxnPreWrite.builder()
                .isolationLevel(IsolationLevel.of(
                    isolationLevel
                ))
                .mutations(Collections.singletonList(cacheToObject.getMutation()))
                .primaryLock(primaryKey)
                .startTs(startTs)
                .lockTtl(TransactionManager.lockTtlTm())
                .txnSize(1L)
                .tryOnePc(false)
                .maxCommitTs(0L)
                .lockExtraDatas(TransactionUtil.toLockExtraDataList(cacheToObject.getTableId(), cacheToObject.getPartId(), txnId,
                    TransactionType.OPTIMISTIC.getCode(), 1))
                .build();
            try {
                StoreInstance store = Services.KV_STORE.getInstance(cacheToObject.getTableId(), cacheToObject.getPartId());
                future = store.txnPreWritePrimaryKey(txnPreWrite, getLockTimeOut());
                break;
            } catch (RegionSplitException e) {
                log.error(e.getMessage(), e);
                CommonId regionId = TransactionUtil.singleKeySplitRegionId(cacheToObject.getTableId(), txnId, cacheToObject.getMutation().getKey());
                cacheToObject.setPartId(regionId);
                sleep();
            }
        }
        if (future == null) {
            throw new RuntimeException(txnId + " future is null " + cacheToObject.getPartId() + ",preWritePrimaryKey false,PrimaryKey:" + primaryKey);
        }
        this.future = future;
    }

    @Override
    public void resolveWriteConflict(JobManager jobManager, Location currentLocation, RuntimeException e) {
        rollback(jobManager);
        throw e;
    }

}
