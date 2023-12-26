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
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.exception.TaskFinException;
import io.dingodb.exec.fin.ErrorType;
import io.dingodb.exec.transaction.base.BaseTransaction;
import io.dingodb.exec.transaction.base.TransactionConfig;
import io.dingodb.exec.transaction.base.TransactionStatus;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.base.CacheToObject;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.exec.transaction.visitor.DingoTransactionRenderJob;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.exception.ReginSplitException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Future;

@Slf4j
public class OptimisticTransaction extends BaseTransaction {
    public OptimisticTransaction(long startTs) {
        super(startTs);
    }

    public OptimisticTransaction(CommonId txnId) {
        super(txnId);
    }

    @Override
    public TransactionType getType() {
        return TransactionType.OPTIMISTIC;
    }

    @Override
    public void cleanUp() {
        if (future != null)
            future.cancel(true);
    }

    public void retryPrepare() {
//        TransactionManager.unregister(txnId);
        long start_ts = TransactionManager.nextTimestamp();
        this.start_ts = start_ts;
//        this.txnInstanceId = new CommonId(CommonId.CommonType.TXN_INSTANCE, start_ts, 0l);
//        this.txnId = new CommonId(CommonId.CommonType.TRANSACTION, TransactionManager.getServerId().seq, start_ts);
//        TransactionManager.register(txnId, this);
        this.status = TransactionStatus.PRE_WRITE_RETRY;
    }

    public Job createRetryJob(JobManager jobManager) {
        long jobSeqId = TransactionManager.nextTimestamp();
        Job job = jobManager.createJob(start_ts, jobSeqId, txnId, null);
        return job;
    }
    public void retryRun(JobManager jobManager, Job job, Location currentLocation) {
        DingoTransactionRenderJob.renderPreWriteJob(job, currentLocation, this, true);
        Iterator<Object[]> iterator = jobManager.createIterator(job, null);
        if (iterator.hasNext()) {
            Object[] next = iterator.next();
        }
    }

    @Override
    public CacheToObject preWritePrimaryKey() {
        // 1、get first key from cache
        CacheToObject cacheToObject = cache.getPrimaryKey();
        byte[] key = cacheToObject.getMutation().getKey();
        primaryKey = key;
        // 2、call sdk preWritePrimaryKey
        TxnPreWrite txnPreWrite = TxnPreWrite.builder().
            isolationLevel(IsolationLevel.of(
                isolationLevel
            ))
            .mutations(Collections.singletonList(cacheToObject.getMutation()))
            .primaryLock(primaryKey)
            .startTs(start_ts)
            .lockTtl(TransactionManager.lockTtlTm())
            .txnSize(1l)
            .tryOnePc(false)
            .maxCommitTs(0l)
            .build();
        Future future = null;
        try {
            StoreInstance store = Services.KV_STORE.getInstance(cacheToObject.getTableId(), cacheToObject.getPartId());
            future = store.txnPreWritePrimaryKey(txnPreWrite);
        } catch (ReginSplitException e) {
            log.error(e.getMessage(), e);
            CommonId regionId = TransactionUtil.singleKeySplitRegionId(cacheToObject.getTableId(), txnId, cacheToObject.getMutation().getKey());
            StoreInstance store = Services.KV_STORE.getInstance(cacheToObject.getTableId(), regionId);
            future = store.txnPreWritePrimaryKey(txnPreWrite);
        }
        if(future == null) {
            throw new RuntimeException(txnId + " future is null " + cacheToObject.getPartId() + ",preWritePrimaryKey false,PrimaryKey:" + primaryKey);
        }
        this.future = future;
        return cacheToObject;
    }

    @Override
    public void resolveWriteConflict(JobManager jobManager, Location currentLocation, RuntimeException e) {
        rollback(jobManager);
        CommonId retryJobId = CommonId.EMPTY_JOB;;
        int txnRetryLimit = TransactionConfig.txn_retry_limit;
        RuntimeException conflictException = e;
        while (TransactionConfig.disable_txn_auto_retry && (txnRetryLimit-- > 0)) {
            try {
                conflictException = null;
                retryPrepare();
                log.info("{} {} retry", txnId, transactionOf());
                Job job = createRetryJob(jobManager);
                retryJobId = job.getJobId();
                retryRun(jobManager, job, currentLocation);
                this.status = TransactionStatus.PRE_WRITE;
                break;
            } catch (TaskFinException e1) {
                conflictException = e1;
                log.info(e1.getMessage(), e1);
                if(e1.getErrorType().equals(ErrorType.WriteConflict)) {
                    rollback(jobManager);
                } else {
                    break;
                }
            } catch (WriteConflictException e2){
                conflictException = e2;
                log.info(e2.getMessage(), e2);
                rollback(jobManager);
            } finally {
                jobManager.removeJob(retryJobId);
            }
        }
        if (conflictException != null) {
            throw conflictException;
        } else {
            log.info("{} {} preWrite retry success", txnId, transactionOf());
        }
    }

}
