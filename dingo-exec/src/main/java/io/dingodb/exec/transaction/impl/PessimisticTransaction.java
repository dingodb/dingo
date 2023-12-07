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
import io.dingodb.exec.exception.TaskFinException;
import io.dingodb.exec.transaction.base.BaseTransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;

import java.util.Collections;

public class PessimisticTransaction extends BaseTransaction {

    public PessimisticTransaction(long startTs) {
        super(startTs);
    }

    public PessimisticTransaction(CommonId txnId) {
        super(txnId);
    }

    @Override
    public TransactionType getType() {
        return TransactionType.PESSIMISTIC;
    }

    @Override
    public void cleanUp() {
        future.cancel(true);
        // PessimisticRollback
    }

    @Override
    public void preWritePrimaryKey() {
        // 1、get first key from cache
        byte[] key = cache.getPrimaryKey();
        primaryKey = key;
        // 2、call sdk preWritePrimaryKey
        TxnPreWrite txnPreWrite = TxnPreWrite.builder().
            isolationLevel(IsolationLevel.of(
                isolationLevel
            )).
            mutations(Collections.singletonList(TransactionCacheToMutation.cacheToMutation(new Object[]{primaryKey}))).
            primary_lock(primaryKey).
            start_ts(start_ts).
            lock_ttl(lockTtl).
            txn_size(1l).
            try_one_pc(false).
            max_commit_ts(0l).
            build();
        part.txnPreWrite(txnPreWrite);
    }

    @Override
    public void resolveWriteConflict(JobManager jobManager, Location currentLocation, TaskFinException e) {
        rollback(jobManager);
        throw e;
    }
}
