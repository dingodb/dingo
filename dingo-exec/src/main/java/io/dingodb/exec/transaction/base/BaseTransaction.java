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
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.exception.TaskFinException;
import io.dingodb.exec.fin.ErrorType;
import io.dingodb.exec.table.Part;
import io.dingodb.exec.transaction.impl.TransactionCache;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.visitor.DingoTransactionRenderJob;
import io.dingodb.meta.MetaService;
import io.dingodb.net.Channel;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

@Slf4j
@Getter
@Setter
@AllArgsConstructor
public abstract class BaseTransaction implements ITransaction{

    protected int isolationLevel = IsolationLevel.ReadCommitted.getCode();
    protected long start_ts;
    protected CommonId txnId;
    protected CommonId txnInstanceId;
    protected boolean closed = false;
    protected boolean isCrossRange = true;
    protected TransactionStatus status;
    protected TransactionCache cache = new TransactionCache();
    protected Map<CommonId, Channel> channelMap;
    protected byte[] primaryKey;
    protected long commit_ts;
    protected long lockTtl = TransactionConfig.lock_ttl;
    protected Job job;
    protected Part part;
    protected Future future;

    public BaseTransaction(@NonNull CommonId txnId) {
        this.txnId = txnId;
        this.start_ts = txnId.seq;
        this.txnInstanceId = new CommonId(CommonId.CommonType.TXN_INSTANCE, txnId.seq, 0l);
        this.status = TransactionStatus.START;
        this.channelMap = new ConcurrentHashMap<>();
        TransactionManager.register(txnId, this);
    }

    public BaseTransaction(long start_ts) {
        this.start_ts = start_ts;
        this.txnInstanceId = new CommonId(CommonId.CommonType.TXN_INSTANCE, start_ts, 0l);
        this.txnId = new CommonId(CommonId.CommonType.TRANSACTION, TransactionManager.getServerId().seq, start_ts);
        this.status = TransactionStatus.START;
        this.channelMap = new ConcurrentHashMap<>();
        TransactionManager.register(txnId, this);
    }

    public abstract TransactionType getType();

    public abstract void cleanUp();

    public abstract void resolveWriteConflict(JobManager jobManager, Location currentLocation, TaskFinException e);

    public abstract void preWritePrimaryKey();

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

    @Override
    public void close() {
        TransactionManager.unregister(txnId);
        this.closed = true;
        this.status = TransactionStatus.CLOSE;
    }

    @Override
    public void registerChannel(CommonId commonId, Channel channel) {
        channelMap.put(commonId, channel);
    }

    @Override
    public boolean commitPrimaryKey() {
        // 1、call sdk commitPrimaryKey
        TxnCommit commitRequest = TxnCommit.builder().
            isolationLevel(IsolationLevel.of(isolationLevel)).
            start_ts(start_ts).
            commit_ts(commit_ts).
            keys(Collections.singletonList(primaryKey)).
            build();
        return part.txnCommit(commitRequest);
    }

    @Override
    public void commit(JobManager jobManager) {
        long preWriteStart = System.nanoTime();
        // begin
        // nothing
        // commit
        if(status == TransactionStatus.START) {
            return;
        }
        Location currentLocation = MetaService.root().currentLocation();
        CommonId jobId = CommonId.EMPTY_JOB;
        this.status= TransactionStatus.PRE_WRITE_START;
        try {
            log.info("{} {} Start PreWritePrimaryKey", txnId, transactionOf());
            // 1、PreWritePrimaryKey 、heartBeat
            preWritePrimaryKey();
            // 2、generator job、task、PreWriteOperator
            long jobSeqId = TransactionManager.nextTimestamp();
            job = jobManager.createJob(start_ts, jobSeqId, txnId, null);
            jobId = job.getJobId();
            DingoTransactionRenderJob.renderPreWriteJob(job, currentLocation, this, true);
            // 3、run PreWrite
            Iterator<Object[]> iterator = jobManager.createIterator(job, null);
            if (iterator.hasNext()) {
                Object[] next = iterator.next();
            }
            this.status = TransactionStatus.PRE_WRITE;
        } catch (TaskFinException e){
            log.info(e.getMessage(), e);
            // rollback or retry
            this.status = TransactionStatus.PRE_WRITE_FAIL;
            if(e.getErrorType().equals(ErrorType.WriteConflict)) {
                resolveWriteConflict(jobManager, currentLocation, e);
            } else {
                throw e;
            }
        } catch (Exception e){
            log.info(e.getMessage(), e);
            this.status = TransactionStatus.PRE_WRITE_FAIL;
            throw e;
        } catch (Throwable t) {
            log.info(t.getMessage(), t);
            this.status = TransactionStatus.PRE_WRITE_FAIL;
            throw new RuntimeException(t);
        } finally {
            log.info("{} {} PreWrite End, Cost:{}ns", txnId, transactionOf(), (System.nanoTime() - preWriteStart));
            jobManager.removeJob(jobId);
        }

        try {
            log.info("{} {} Start CommitPrimaryKey", txnId, transactionOf());
            // 4、get commit_ts 、CommitPrimaryKey
            this.commit_ts = TransactionManager.getCommit_ts();
            commitPrimaryKey();
            // 5、generator job、task、CommitOperator
            job = jobManager.createJob(start_ts, commit_ts, txnId, null);
            jobId = job.getJobId();
            DingoTransactionRenderJob.renderCommitJob(job, currentLocation, this, true);
            // 6、run Commit
            Iterator<Object[]> iterator = jobManager.createIterator(job, null);
            if (iterator.hasNext()) {
                Object[] next = iterator.next();
            }
            this.status = TransactionStatus.COMMIT;
        } catch (Throwable t) {
            log.info(t.getMessage(), t);
            this.status = TransactionStatus.COMMIT_FAIL;
            throw new RuntimeException(t);
        } finally {
            log.info("{} {} Commit End, Cost:{}ns", txnId, transactionOf(), (System.nanoTime() - preWriteStart));
            jobManager.removeJob(jobId);
            cleanUp();
        }
    }

    @Override
    public void rollback(JobManager jobManager) {
        long rollBackStart = System.nanoTime();
        log.info("{} {} RollBack Start", txnId, transactionOf());
        Location currentLocation = MetaService.root().currentLocation();
        CommonId jobId = CommonId.EMPTY_JOB;
        try {
            // 1、get commit_ts
            long rollBack_ts= TransactionManager.nextTimestamp();
            // 2、generator job、task、RollBackOperator
            job = jobManager.createJob(start_ts, rollBack_ts, txnId, null);
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
            log.info("{} {} RollBack End, Cost:{}ns", txnId, transactionOf(), (System.nanoTime() - rollBackStart));
            jobManager.removeJob(jobId);
            cleanUp();
        }
    }

}
