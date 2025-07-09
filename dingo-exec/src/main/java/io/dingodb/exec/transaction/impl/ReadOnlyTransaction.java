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
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.log.MdcUtils;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.transaction.base.BaseTransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.base.TwoPhaseCommitData;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.Future;

@Slf4j
public class ReadOnlyTransaction extends BaseTransaction {

    @Getter
    private final TransactionType transactionType;

    @Getter
    @Setter
    private long forUpdateTs = 0L;

    public ReadOnlyTransaction(@NonNull TransactionType trxType, @NonNull CommonId txnId, int isolationLevel) {
        super(txnId, isolationLevel);
        this.transactionType = trxType;
    }

    public ReadOnlyTransaction(@NonNull TransactionType trxType, long startTs, int isolationLevel) {
        super(startTs, isolationLevel);
        this.transactionType = trxType;
    }

    @Override
    public synchronized void commit(JobManager jobManager) {
    }

    @Override
    public void cleanUp(JobManager jobManager) {
    }

    @Override
    public synchronized void rollback(JobManager jobManager) {
    }

    @Override
    public String transactionOf() {
        TransactionType type = getType();
        switch (type) {
            case PESSIMISTIC:
                return "ReadOnly PessimisticTransaction";
            case OPTIMISTIC:
                return "ReadOnly OptimisticTransaction";
            case NONE:
                return "None";
        }
        throw new RuntimeException(txnId + "The transaction type is " + type + " no support");
    }

    @Override
    public TransactionType getType() {
        return transactionType;
    }

    @Override
    public void resolveWriteConflict(JobManager jobManager, Location currentLocation, RuntimeException exception) {
        throw exception;
    }

    @Override
    public void preWritePrimaryKey(TwoPhaseCommitData twoPhaseCommitData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean onePcStage() {
        return false;
    }

    @Override
    public void rollBackResidualPessimisticLock(JobManager jobManager) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollBackPessimisticLock(JobManager jobManager) {
    }

    @Override
    public void rollBackOptimisticCurrentJobData(JobManager jobManager) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cleanOptimisticCurrentJobData(JobManager jobManager) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollBackPessimisticPrimaryLock(JobManager jobManager) {
    }

    @Override
    public byte[] getPrimaryKeyLock() {
        return null;
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
    public long getJobSeqId() {
        return 0;
    }

}
