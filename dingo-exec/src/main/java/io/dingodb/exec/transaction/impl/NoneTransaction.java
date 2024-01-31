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
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.transaction.base.BaseTransaction;
import io.dingodb.exec.transaction.base.CacheToObject;
import io.dingodb.exec.transaction.base.TransactionType;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Future;

@Slf4j
public class NoneTransaction extends BaseTransaction {
    public NoneTransaction(long startTs, int isolationLevel) {
        super(startTs, isolationLevel);
    }

    public NoneTransaction(CommonId txnId, int isolationLevel) {
        super(txnId, isolationLevel);
    }

    @Override
    public TransactionType getType() {
        return TransactionType.NONE;
    }

    @Override
    public void resolveWriteConflict(JobManager jobManager, Location currentLocation, RuntimeException e) {

    }

    @Override
    public CacheToObject preWritePrimaryKey() {
        return null;
    }

    @Override
    public void rollBackPessimisticLock(JobManager jobManager) {

    }

    @Override
    public long getForUpdateTs() {
        return 0;
    }

    @Override
    public void setForUpdateTs(long forUpdateTs) {

    }

    @Override
    public byte[] getPrimaryKeyLock() {
        return new byte[0];
    }

    @Override
    public void setPrimaryKeyLock(byte[] primaryKeyLock) {

    }

    @Override
    public void setPrimaryKeyFuture(Future future) {

    }

    @Override
    public synchronized void commit(JobManager jobManager) {
        cleanUp(jobManager);
    }

    @Override
    public synchronized void rollback(JobManager jobManager) {
        cleanUp(jobManager);
    }
}
