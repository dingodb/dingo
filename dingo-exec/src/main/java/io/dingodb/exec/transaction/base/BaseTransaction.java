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

package io.dingodb.exec.transaction.base;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.exception.TaskFinException;
import io.dingodb.exec.fin.ErrorType;
import io.dingodb.exec.transaction.impl.TransactionCache;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.transaction.visitor.DingoTransactionRenderJob;
import io.dingodb.meta.MetaService;
import io.dingodb.net.Channel;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.exception.CommitTsExpiredException;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Getter
@Setter
@AllArgsConstructor
public abstract class BaseTransaction implements ITransaction {

    protected int isolationLevel;
    protected long startTs;
    protected CommonId txnId;
    protected CommonId txnInstanceId;
    protected boolean closed = false;
    protected boolean isCrossNode = false;
    protected TransactionStatus status;
    protected TransactionCache cache;
    protected Map<CommonId, Channel> channelMap;
    protected byte[] primaryKey;
    protected long commitTs;
    protected Job job;
    protected Future future;
    protected List<String> sqlList;
    protected boolean autoCommit;
    protected TransactionConfig transactionConfig;
    protected Future commitFuture;
    protected CacheToObject cacheToObject;
    protected AtomicBoolean cancel;

    protected CompletableFuture<Void> finishedFuture = new CompletableFuture<>();

    public BaseTransaction(@NonNull CommonId txnId, int isolationLevel) {
        this.isolationLevel = isolationLevel;
        this.txnId = txnId;
        this.startTs = txnId.seq;
        this.txnInstanceId = new CommonId(CommonId.CommonType.TXN_INSTANCE, txnId.seq, 0L);
        this.status = TransactionStatus.START;
        this.cancel = new AtomicBoolean(false);
        this.channelMap = new ConcurrentHashMap<>();
        this.cache = new TransactionCache(txnId);
        this.sqlList = new ArrayList<>();
        this.transactionConfig = new TransactionConfig();
        TransactionManager.register(txnId, this);
    }

    public BaseTransaction(long startTs, int isolationLevel) {
        this.isolationLevel = isolationLevel;
        this.startTs = startTs;
        this.txnInstanceId = new CommonId(CommonId.CommonType.TXN_INSTANCE, startTs, 0L);
        this.txnId = new CommonId(CommonId.CommonType.TRANSACTION, TransactionManager.getServerId().seq, startTs);
        this.status = TransactionStatus.START;
        this.cancel = new AtomicBoolean(false);
        this.channelMap = new ConcurrentHashMap<>();
        this.cache = new TransactionCache(txnId);
        this.sqlList = new ArrayList<>();
        this.transactionConfig = new TransactionConfig();
        TransactionManager.register(txnId, this);
    }

    @Override
    public void addSql(String sql) {
        sqlList.add(sql);
    }

    @Override
    public void setTransactionConfig(Properties sessionVariables) {
        transactionConfig.setSessionVariables(sessionVariables);
    }

    @Override
    public long getLockTimeOut() {
        return transactionConfig.getLockTimeOut();
    }

    public boolean isPessimistic() {
        TransactionType type = getType();
        switch (type) {
            case PESSIMISTIC:
                return true;
        }
        return false;
    }

    protected void sleep() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void cleanUp(JobManager jobManager) {
        if (future != null) {
            future.cancel(true);
        }
        finishedFuture.complete(null);
        log.info("{} cleanUp finishedFuture the current {} end", txnId, transactionOf());
        if (getType() == TransactionType.NONE) {
            return;
        }
        if (getSqlList().size() == 0 || !cache.checkCleanContinue(isPessimistic())) {
            log.warn("{} The current {} has no data to cleanUp", txnId, transactionOf());
            return;
        }
        Location currentLocation = MetaService.root().currentLocation();
        CompletableFuture<Void> cleanUp_future = CompletableFuture.runAsync(() ->
            cleanUpJobRun(jobManager, currentLocation), Executors.executor("exec-txnCleanUp")
        ).exceptionally(
            ex -> {
                log.error(ex.toString(), ex);
                return null;
            }
        );
    }

    public abstract void resolveWriteConflict(JobManager jobManager, Location currentLocation, RuntimeException e);

    public abstract void preWritePrimaryKey();

    public abstract void rollBackResidualPessimisticLock(JobManager jobManager);

    public String transactionOf() {
        TransactionType type = getType();
        switch (type) {
            case PESSIMISTIC:
                return "PessimisticTransaction";
            case OPTIMISTIC:
                return "OptimisticTransaction";
        }
        return "PessimisticTransaction";
    }

    protected void checkContinue() {
        if (cancel.get()) {
            log.info("{} The current {} has been canceled", txnId, transactionOf());
            throw new RuntimeException(txnId + "The transaction has been canceled");
        }
    }

    @Override
    public void cancel() {
        cancel.compareAndSet(false, true);
        log.info("{} The current {} cancel is set to true", txnId, transactionOf());
    }

    @Override
    public synchronized void close(JobManager jobManager) {
        cleanUp(jobManager);
        TransactionManager.unregister(txnId);
        this.closed = true;
        this.status = TransactionStatus.CLOSE;
    }

    @Override
    public void registerChannel(CommonId commonId, Channel channel) {
        channelMap.put(commonId, channel);
        log.info("{} {} isCrossNode commonId is {} location is {}", txnId, transactionOf(),
            commonId, channel.remoteLocation());
        isCrossNode = true;
    }

    @Override
    public boolean commitPrimaryKey(CacheToObject cacheToObject) {
        try {
            // 1、call sdk commitPrimaryKey
            long start = System.currentTimeMillis();
            while (true) {
                TxnCommit commitRequest = TxnCommit.builder()
                    .isolationLevel(IsolationLevel.of(isolationLevel))
                    .startTs(startTs)
                    .commitTs(commitTs)
                    .keys(Collections.singletonList(primaryKey))
                    .build();
                try {
                    StoreInstance store = Services.KV_STORE.getInstance(cacheToObject.getTableId(), cacheToObject.getPartId());
                    return store.txnCommit(commitRequest);
                } catch (RegionSplitException e) {
                    log.error(e.getMessage(), e);
                    // 2、regin split
                    CommonId regionId = TransactionUtil.singleKeySplitRegionId(cacheToObject.getTableId(), txnId, primaryKey);
                    cacheToObject.setPartId(regionId);
                    sleep();
                } catch (CommitTsExpiredException e) {
                    log.error(e.getMessage(), e);
                    this.commitTs = TransactionManager.getCommitTs();
                }
                long elapsed = System.currentTimeMillis() - start;
                if (elapsed > getLockTimeOut()) {
                    return false;
                }
            }
        } catch (Throwable throwable) {
            log.error(throwable.getMessage(), throwable);
        }
        return false;
    }

    @Override
    public synchronized void commit(JobManager jobManager) {
        // begin
        // nothing
        // commit
        log.info("{} {} Start commit", txnId, transactionOf());
        if (status != TransactionStatus.START) {
            throw new RuntimeException(txnId + ":" + transactionOf() + " unavailable status is " + status);
        }
        if (getType() == TransactionType.NONE) {
            return;
        }
        checkContinue();
        if (getSqlList().size() == 0 || !cache.checkContinue()) {
            log.warn("{} The current {} has no data to commit", txnId, transactionOf());
            if (isPessimistic()) {
                // PessimisticRollback
                rollBackResidualPessimisticLock(jobManager);
            }
            return;
        }
        long preWriteStart = System.currentTimeMillis();
        Location currentLocation = MetaService.root().currentLocation();
        AtomicReference<CommonId> jobId = new AtomicReference<>(CommonId.EMPTY_JOB);
        this.status = TransactionStatus.PRE_WRITE_START;
        try {
            log.info("{} {} Start PreWritePrimaryKey", txnId, transactionOf());
            checkContinue();
            // 1、PreWritePrimaryKey 、heartBeat
            preWritePrimaryKey();
            if (cacheToObject.getMutation().getOp() == Op.CheckNotExists) {
                log.info("{} {} PreWritePrimaryKey Op is CheckNotExists", txnId, transactionOf());
                return;
            }
            checkContinue();
            // 2、generator job、task、PreWriteOperator
            long jobSeqId = TransactionManager.nextTimestamp();
            job = jobManager.createJob(startTs, jobSeqId, txnId, null);
            jobId.set(job.getJobId());
            DingoTransactionRenderJob.renderPreWriteJob(job, currentLocation, this, true);
            // 3、run PreWrite
            Iterator<Object[]> iterator = jobManager.createIterator(job, null);
            while (iterator.hasNext()) {
                Object[] next = iterator.next();
            }
            this.status = TransactionStatus.PRE_WRITE;
        } catch (WriteConflictException e) {
            log.info(e.getMessage(), e);
            // rollback or retry
            this.status = TransactionStatus.PRE_WRITE_FAIL;
            resolveWriteConflict(jobManager, currentLocation, e);
        } catch (DuplicateEntryException e) {
            log.info(e.getMessage(), e);
            // rollback
            this.status = TransactionStatus.PRE_WRITE_FAIL;
            rollback(jobManager);
            throw e;
        } catch (TaskFinException e) {
            log.info(e.getMessage(), e);
            // rollback or retry
            this.status = TransactionStatus.PRE_WRITE_FAIL;
            if (e.getErrorType().equals(ErrorType.WriteConflict)) {
                resolveWriteConflict(jobManager, currentLocation, e);
            } else if (e.getErrorType().equals(ErrorType.DuplicateEntry)) {
                rollback(jobManager);
                throw e;
            } else {
                rollback(jobManager);
                throw e;
            }
        } catch (Exception e) {
            log.info(e.getMessage(), e);
            this.status = TransactionStatus.PRE_WRITE_FAIL;
            rollback(jobManager);
            throw e;
        } catch (Throwable t) {
            log.info(t.getMessage(), t);
            this.status = TransactionStatus.PRE_WRITE_FAIL;
            rollback(jobManager);
            throw new RuntimeException(t);
        } finally {
            if (cancel.get()) {
                this.status = TransactionStatus.CANCEL;
            }
            log.info("{} {} PreWrite End Status:{}, Cost:{}ms", txnId, transactionOf(),
                status, (System.currentTimeMillis() - preWriteStart));
            jobManager.removeJob(jobId.get());
        }

        if (isPessimistic()) {
            // PessimisticRollback
            rollBackResidualPessimisticLock(jobManager);
        }

        try {
            if (cancel.get()) {
                log.info("{} The current {} has been canceled", txnId, transactionOf());
                rollback(jobManager);
                throw new RuntimeException(txnId + "The transaction has been canceled");
            }
            log.info("{} {} Start CommitPrimaryKey", txnId, transactionOf());
            // 4、get commit_ts 、CommitPrimaryKey
            this.commitTs = TransactionManager.getCommitTs();
            boolean result = commitPrimaryKey(cacheToObject);
            if (!result) {
                rollback(jobManager);
                throw new RuntimeException(txnId + " " + cacheToObject.getPartId()
                    + ",txnCommitPrimaryKey false, commit_ts:" + commitTs +",PrimaryKey:"
                    + primaryKey.toString());
            }
            CompletableFuture<Void> commit_future = CompletableFuture.runAsync(() ->
                commitJobRun(jobManager, currentLocation), Executors.executor("exec-txnCommit")
            ).exceptionally(
                ex -> {
                    log.error(ex.toString(), ex);
                    return null;
                }
            );
            commitFuture = commit_future;
            if (!cancel.get()) {
                commit_future.get();
            }
            this.status = TransactionStatus.COMMIT;
        } catch (Throwable t) {
            log.info(t.getMessage(), t);
            this.status = TransactionStatus.COMMIT_FAIL;
            throw new RuntimeException(t);
        } finally {
            if (cancel.get()) {
                this.status = TransactionStatus.CANCEL;
            }
            log.info("{} {} Commit End Status:{}, Cost:{}ms", txnId, transactionOf(),
                status, (System.currentTimeMillis() - preWriteStart));
            jobManager.removeJob(jobId.get());
            if (!cancel.get()) {
                commitFuture = null;
            }
//            cleanUp();
        }
    }

    private void cleanUpJobRun(JobManager jobManager, Location currentLocation) {
        CommonId jobId = CommonId.EMPTY_JOB;
        try {
            // 1、getTso
            long cleanUpTs = TransactionManager.nextTimestamp();
            // 2、generator job、task、cleanCacheOperator
            Job job = jobManager.createJob(startTs, cleanUpTs, txnId, null);
            jobId = job.getJobId();
            DingoTransactionRenderJob.renderCleanCacheJob(job, currentLocation, this, true);
            // 3、run cleanCache
            if (commitFuture != null) {
                commitFuture.get();
            }
            Iterator<Object[]> iterator = jobManager.createIterator(job, null);
            while (iterator.hasNext()) {
                Object[] next = iterator.next();
            }
            log.info("{} {} cleanUpJobRun end", txnId, transactionOf());
        } catch (Throwable throwable) {
            log.error(throwable.getMessage(), throwable);
        } finally {
            jobManager.removeJob(jobId);
        }
    }

    private void commitJobRun(JobManager jobManager, Location currentLocation) {
        CommonId jobId = CommonId.EMPTY_JOB;
        try {
            // 5、generator job、task、CommitOperator
            job = jobManager.createJob(startTs, commitTs, txnId, null);
            jobId = job.getJobId();
            DingoTransactionRenderJob.renderCommitJob(job, currentLocation, this, true);
            // 6、run Commit
            Iterator<Object[]> iterator = jobManager.createIterator(job, null);
            while (iterator.hasNext()) {
                Object[] next = iterator.next();
            }
            log.info("{} {} commitJobRun end", txnId, transactionOf());
        } catch (Throwable throwable) {
            log.error(throwable.getMessage(), throwable);
        } finally {
            jobManager.removeJob(jobId);
        }
    }

    @Override
    public synchronized void rollback(JobManager jobManager) {
        if (getType() == TransactionType.NONE) {
            return;
        }
        if (getSqlList().size() == 0 || !cache.checkContinue()) {
            log.warn("{} The current {} has no data to rollback",txnId, transactionOf());
            return;
        }
        long rollBackStart = System.currentTimeMillis();
        log.info("{} {} RollBack Start", txnId, transactionOf());
        Location currentLocation = MetaService.root().currentLocation();
        CommonId jobId = CommonId.EMPTY_JOB;
        try {
            // 1、get commit_ts
            long rollbackTs = TransactionManager.nextTimestamp();
            // 2、generator job、task、RollBackOperator
            job = jobManager.createJob(startTs, rollbackTs, txnId, null);
            jobId = job.getJobId();
            DingoTransactionRenderJob.renderRollBackJob(job, currentLocation, this, true);
            // 3、run RollBack
            Iterator<Object[]> iterator = jobManager.createIterator(job, null);
            this.status = TransactionStatus.ROLLBACK;
        } catch (Throwable t) {
            log.info(t.getMessage(), t);
            this.status = TransactionStatus.ROLLBACK_FAIL;
            throw new RuntimeException(t);
        } finally {
            log.info("{} {} RollBack End Status:{}, Cost:{}ms", txnId, transactionOf(),
                status, (System.currentTimeMillis() - rollBackStart));
            jobManager.removeJob(jobId);
//            cleanUp();
        }
    }

}
