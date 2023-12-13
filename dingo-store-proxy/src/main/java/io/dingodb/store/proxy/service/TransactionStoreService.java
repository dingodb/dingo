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

package io.dingodb.store.proxy.service;

import io.dingodb.common.CommonId;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.sdk.service.MetaService;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.sdk.service.entity.store.LockInfo;
import io.dingodb.sdk.service.entity.store.TxnBatchRollbackResponse;
import io.dingodb.sdk.service.entity.store.TxnCheckTxnStatusResponse;
import io.dingodb.sdk.service.entity.store.TxnCommitResponse;
import io.dingodb.sdk.service.entity.store.TxnHeartBeatRequest;
import io.dingodb.sdk.service.entity.store.TxnPrewriteResponse;
import io.dingodb.sdk.service.entity.store.TxnResolveLockResponse;
import io.dingodb.sdk.service.entity.store.TxnResultInfo;
import io.dingodb.sdk.service.entity.store.WriteConflict;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.checkstatus.TxnCheckStatus;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.data.resolvelock.TxnResolveLock;
import io.dingodb.store.api.transaction.data.rollback.TxnBatchRollBack;
import io.dingodb.store.api.transaction.exception.PrimaryMismatchException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.Future;

import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class TransactionStoreService {

    private final MetaService metaService;
    private final StoreService storeService;

    public TransactionStoreService(MetaService metaService, StoreService storeService) {
        this.metaService = metaService;
        this.storeService = storeService;
    }

    public void heartbeat(TxnPreWrite txnPreWrite) {
        storeService.txnHeartBeat(TxnHeartBeatRequest.builder()
            .primaryLock(txnPreWrite.getPrimaryLock())
            .startTs(txnPreWrite.getStartTs())
            .adviseLockTtl(SECONDS.toMillis(5))
            .build()
        );
    }

    public boolean txnPreWrite(StoreService storeService, TxnPreWrite txnPreWrite) {
        txnPreWrite.getMutations().forEach($ -> $.getKey()[0] = 't');
        int retry = 30;
        IsolationLevel isolationLevel = txnPreWrite.getIsolationLevel();
        while (retry-- > 0) {
            TxnPrewriteResponse response = storeService.txnPrewrite(MAPPER.preWriteTo(txnPreWrite));
            if (response.getTxnResult() == null || response.getTxnResult().isEmpty()) {
                return true;
            }
            resolveConflict(response.getTxnResult(), isolationLevel.getCode(), txnPreWrite.getStartTs());
        }
        return false;
    }

    public Future txnPreWritePrimaryKey(StoreService storeService, TxnPreWrite txnPreWrite) {
        if (txnPreWrite(storeService, txnPreWrite)) {
            return Executors.scheduleWithFixedDelayAsync("txn-heartbeat", () -> heartbeat(txnPreWrite), 1, 1, SECONDS);
        }
        throw new WriteConflictException();
    }

    public boolean txnCommit(StoreService storeService, TxnCommit txnCommit) {
        txnCommit.getKeys().forEach($ -> $[0] = 't');
        TxnCommitResponse response = storeService.txnCommit(MAPPER.commitTo(txnCommit));
        return response.getTxnResult() == null;
    }

    public boolean txnBatchRollback(StoreService storeService, TxnBatchRollBack txnBatchRollBack) {
        txnBatchRollBack.getKeys().forEach($ -> $[0] = 't');
        TxnBatchRollbackResponse response = storeService.txnBatchRollback(MAPPER.rollbackTo(txnBatchRollBack));
        return response.getTxnResult() == null;
    }

    public TxnCheckTxnStatusResponse txnCheckTxnStatus(CommonId tableId, TxnCheckStatus txnCheckStatus) {
        // TODO
        return null;
    }

    public TxnResolveLockResponse txnResolveLock(TxnResolveLock txnResolveLock) {
        // TODO
        return null;
    }

    private void resolveConflict(List<TxnResultInfo> txnResult, int isolationLevel, long startTs) {
        for (TxnResultInfo txnResultInfo : txnResult) {
            log.info("txnPreWrite txnResultInfo :" + txnResultInfo);
            LockInfo lockInfo = txnResultInfo.getLocked();
            if (lockInfo != null) {
                // CheckTxnStatus
                log.info("txnPreWrite lockInfo :" + lockInfo);
                long currentTs = TsoService.INSTANCE.tso();
                TxnCheckStatus txnCheckStatus = TxnCheckStatus.builder().
                    isolationLevel(IsolationLevel.of(isolationLevel)).
                    primaryKey(lockInfo.getPrimaryLock()).
                    lockTs(lockInfo.getLockTs()).
                    callerStartTs(startTs).
                    currentTs(currentTs).
                    build();
                TxnCheckTxnStatusResponse statusResponse = txnCheckTxnStatus(null, txnCheckStatus);
                log.info("txnPreWrite txnCheckStatus :" + statusResponse);
                TxnResultInfo resultInfo = statusResponse.getTxnResult();
                // success
                if (resultInfo == null) {
                    long lockTtl = statusResponse.getLockTtl();
                    long commitTs = statusResponse.getCommitTs();
                    if (lockTtl > 0) {
                        // wait
                        try {
                            Thread.sleep(lockTtl);
                            log.info("lockInfo wait " + lockTtl + "ms end.");
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (commitTs > 0) {
                        // resolveLock store commit
                        TxnResolveLock resolveLockRequest = TxnResolveLock.builder().
                            isolationLevel(IsolationLevel.of(isolationLevel)).
                            startTs(lockInfo.getLockTs()).
                            commitTs(commitTs).
                            keys(singletonList(lockInfo.getKey())).
                            build();
                        TxnResolveLockResponse txnResolveLockRes = txnResolveLock(resolveLockRequest);
                        log.info("txnResolveLockResponse:" + txnResolveLockRes);
                    } else if (lockTtl == 0 && commitTs == 0) {
                        // resolveLock store rollback
                        TxnResolveLock resolveLockRequest = TxnResolveLock.builder().
                            isolationLevel(IsolationLevel.of(isolationLevel)).
                            startTs(lockInfo.getLockTs()).
                            commitTs(commitTs).
                            keys(singletonList(lockInfo.getKey())).
                            build();
                        TxnResolveLockResponse txnResolveLockRes = txnResolveLock(resolveLockRequest);
                        log.info("txnResolveLockResponse:" + txnResolveLockRes);
                    }
                } else {
                    // 1、PrimaryMismatch  or  TxnNotFound
                    if (resultInfo.getPrimaryMismatch() != null) {
                        throw new PrimaryMismatchException(resultInfo.getPrimaryMismatch().toString());
                    } else if (resultInfo.getTxnNotFound() != null) {
                        throw new RuntimeException(resultInfo.getTxnNotFound().toString());
                    }
                }
            } else {
                WriteConflict writeConflict = txnResultInfo.getWriteConflict();
                log.info("txnPreWrite writeConflict :" + writeConflict);
                if (writeConflict != null) {
                    throw new WriteConflictException(writeConflict.toString());
                }
            }
        }
    }

}
