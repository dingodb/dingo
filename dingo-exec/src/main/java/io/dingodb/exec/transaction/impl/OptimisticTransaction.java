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
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.log.MdcUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.transaction.base.BaseTransaction;
import io.dingodb.exec.transaction.base.CacheToObject;
import io.dingodb.exec.transaction.base.TransactionStatus;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.transaction.visitor.DingoTransactionRenderJob;
import io.dingodb.meta.MetaService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.exception.OnePcDegenerateTwoPcException;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

@Slf4j
public class OptimisticTransaction extends BaseTransaction {

    @Getter
    @Setter
    private long jobSeqId;

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
    public synchronized void rollBackOptimisticCurrentJobData(JobManager jobManager) {
        MdcUtils.setTxnId(txnId.toString());
        long rollBackStart = System.currentTimeMillis();
        LogUtils.info(log, "rollBackOptimisticCurrentJobData jobId:{}", job.getJobId());
        cache.setJobId(job.getJobId());
        if(!cache.checkOptimisticLockContinue()) {
            LogUtils.warn(log, "The current {} has no data to rollBackOptimisticCurrentJobData", transactionOf());
            return;
        }
        LogUtils.info(log, "{} rollBackOptimisticCurrentJobData Start", transactionOf());
        Location currentLocation = MetaService.root().currentLocation();
        CommonId jobId = CommonId.EMPTY_JOB;
        long jobSeqId = job.getJobId().seq;
        this.setJobSeqId(jobSeqId);
        try {
            this.status = TransactionStatus.ROLLBACK_OPTIMISTIC_DATA_START;
            // 1、get rollback_ts
            long rollBackTs = TransactionManager.nextTimestamp();
            // 2、generator job、task、rollBackOptimisticOperator
            job = jobManager.createJob(startTs, rollBackTs, txnId, null);
            jobId = job.getJobId();
            DingoTransactionRenderJob.renderRollBackOptimisticData(job, currentLocation, this, true);
            // 3、run RollBackOptimisticLock
            Iterator<Object[]> iterator = jobManager.createIterator(job, null);
            while (iterator.hasNext()) {
                Object[] next = iterator.next();
            }
            this.status = TransactionStatus.ROLLBACK_OPTIMISTIC_DATA;
        } catch (Throwable t) {
            LogUtils.error(log, t.getMessage(), t);
            this.status = TransactionStatus.ROLLBACK_OPTIMISTIC_DATA_FAIL;
            throw new RuntimeException(t);
        } finally {
            this.status = TransactionStatus.START;
            LogUtils.info(log, "{}  rollBackOptimisticCurrentJobData End Status:{}, Cost:{}ms",
                transactionOf(), status, (System.currentTimeMillis() - rollBackStart));
            jobManager.removeJob(jobId);
            MdcUtils.removeTxnId();
        }
    }

    @Override
    public synchronized void cleanOptimisticCurrentJobData(JobManager jobManager) {
        MdcUtils.setTxnId(txnId.toString());
        long rollBackStart = System.currentTimeMillis();
        LogUtils.info(log, "cleanOptimisticCurrentJobData jobId:{}", job.getJobId());
        cache.setJobId(job.getJobId());
        if(!cache.checkOptimisticLockContinue()) {
            LogUtils.warn(log, "The current {} has no data to cleanOptimisticCurrentJobData", transactionOf());
            return;
        }
        LogUtils.info(log, "{} rollBackOptimisticCurrentJobData Start", transactionOf());
        Location currentLocation = MetaService.root().currentLocation();
        CommonId jobId = CommonId.EMPTY_JOB;
        long jobSeqId = job.getJobId().seq;
        this.setJobSeqId(jobSeqId);
        try {
            this.status = TransactionStatus.CLEAN_OPTIMISTIC_DATA_START;
            // 1、get rollback_ts
            long rollBackTs = TransactionManager.nextTimestamp();
            // 2、generator job、task、rollBackOptimisticOperator
            job = jobManager.createJob(startTs, rollBackTs, txnId, null);
            jobId = job.getJobId();
            DingoTransactionRenderJob.renderRollBackOptimisticData(job, currentLocation, this, true);
            // 3、run RollBackOptimisticLock
            Iterator<Object[]> iterator = jobManager.createIterator(job, null);
            while (iterator.hasNext()) {
                Object[] next = iterator.next();
            }
            this.status = TransactionStatus.CLEAN_OPTIMISTIC_DATA;
        } catch (Throwable t) {
            LogUtils.error(log, t.getMessage(), t);
            this.status = TransactionStatus.CLEAN_OPTIMISTIC_DATA_FAIL;
            throw new RuntimeException(t);
        } finally {
            this.status = TransactionStatus.START;
            LogUtils.info(log, "{}  cleanOptimisticCurrentJobData End Status:{}, Cost:{}ms",
                transactionOf(), status, (System.currentTimeMillis() - rollBackStart));
            jobManager.removeJob(jobId);
            MdcUtils.removeTxnId();
        }
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

    /**
     * Run 1pc stage.
     * @return
     *      true: 1PC stage success.
     *      false: Need degenerate to 2PC transaction.
     */
    @Override
    public boolean onePcStage() {
        Iterator<Object[]> transform = cache.iterator();
        List<Mutation> mutations = new ArrayList<Mutation>();
        Set<CommonId> partIdSet = new HashSet<CommonId>();
        boolean forSamePart = true;

        //get primary key.
        cacheToObject = cache.getPrimaryKey();
        byte[] key = cacheToObject.getMutation().getKey();
        primaryKey = key;

        while (transform.hasNext()) {
            Object[] next = transform.next();
            TxnLocalData txnLocalData = (TxnLocalData) next[0];

            //check whether having same patition id.
            partIdSet.add(txnLocalData.getPartId());
            if(partIdSet.size() > 1) {
                forSamePart = false;
                break;
            } else {
                //build Mutaions.
                mutations.add(TransactionCacheToMutation.localDatatoMutation(txnLocalData, TransactionType.OPTIMISTIC));
            }
        }

        if(forSamePart) {
            //commit 1pc.
            return txnOnePCCommit(mutations);
        } else {
            return false;
        }
    }

    private boolean txnOnePCCommit(List<Mutation> mutations) {
        TxnPreWrite txnPreWrite = TxnPreWrite.builder()
            .isolationLevel(IsolationLevel.of(
                isolationLevel
            ))
            .mutations(mutations)
            .primaryLock(primaryKey)
            .startTs(startTs)
            .lockTtl(TransactionManager.lockTtlTm())
            .txnSize(mutations.size())
            .tryOnePc(true)
            .maxCommitTs(0L)
            .lockExtraDatas(TransactionUtil.toLockExtraDataList(cacheToObject.getTableId(), cacheToObject.getPartId(), txnId,
                TransactionType.OPTIMISTIC.getCode(), 1))
            .build();
        try {
            StoreInstance store = Services.KV_STORE.getInstance(cacheToObject.getTableId(), cacheToObject.getPartId());
            long lockTimeOut = getLockTimeOut();
            store.txnPreWrite(txnPreWrite, lockTimeOut);
        } catch (RegionSplitException e) {
            LogUtils.info(log, "Received RegionSplitException exception, so degenerate to 2PC.");
            throw new OnePcDegenerateTwoPcException("1PC degenerate to 2PC, startTs:" + startTs);
        }
        return true;
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
                LogUtils.error(log, e.getMessage(), e);
                CommonId regionId = TransactionUtil.singleKeySplitRegionId(cacheToObject.getTableId(), txnId, cacheToObject.getMutation().getKey());
                cacheToObject.setPartId(regionId);
                Utils.sleep(100);
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
