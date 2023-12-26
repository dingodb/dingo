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
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.transaction.base.BaseTransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.base.CacheToObject;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.exception.ReginSplitException;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

@Slf4j
public class PessimisticTransaction extends BaseTransaction {

    private long for_update_ts;

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
        if(future != null) {
            future.cancel(true);
        }
        // PessimisticRollback
    }

    @Override
    public CacheToObject preWritePrimaryKey() {
        // 1、get first key from cache
        CacheToObject cacheToObject = cache.getPrimaryKey();
        primaryKey = cacheToObject.getMutation().getKey();
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
        try {
            StoreInstance store = Services.KV_STORE.getInstance(cacheToObject.getTableId(), cacheToObject.getPartId());
            boolean result = store.txnPreWrite(txnPreWrite);
            if (!result) {
                throw new RuntimeException(txnId + " " + cacheToObject.getPartId() + ",preWritePrimaryKey false,PrimaryKey:" + primaryKey);
            }
        } catch (ReginSplitException e) {
            log.error(e.getMessage(), e);
            CommonId regionId = TransactionUtil.singleKeySplitRegionId(cacheToObject.getTableId(), txnId, cacheToObject.getMutation().getKey());
            StoreInstance store = Services.KV_STORE.getInstance(cacheToObject.getTableId(), regionId);
            boolean result = store.txnPreWrite(txnPreWrite);
            if (!result) {
                throw new RuntimeException(txnId + " " + regionId + ",preWritePrimaryKey false,PrimaryKey:" + primaryKey);
            }
        }
        return cacheToObject;
    }

    @Override
    public void resolveWriteConflict(JobManager jobManager, Location currentLocation, RuntimeException e) {
        rollback(jobManager);
        throw e;
    }
}
