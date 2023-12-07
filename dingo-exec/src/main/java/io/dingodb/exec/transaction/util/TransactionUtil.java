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

package io.dingodb.exec.transaction.util;

import io.dingodb.exec.table.Part;
import io.dingodb.store.api.transaction.exception.PrimaryMismatchException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.LockInfo;
import io.dingodb.store.api.transaction.data.TxnResultInfo;
import io.dingodb.store.api.transaction.data.WriteConflict;
import io.dingodb.store.api.transaction.data.checkstatus.TxnCheckStatus;
import io.dingodb.store.api.transaction.data.checkstatus.TxnCheckStatusResult;
import io.dingodb.store.api.transaction.data.resolvelock.TxnResolveLock;
import io.dingodb.store.api.transaction.data.resolvelock.TxnResolveLockResult;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class TransactionUtil {
    public static void resolveConflict(@NonNull List<TxnResultInfo> txnResult, int isolationLevel, long start_ts, @NonNull Part part) {
        for (TxnResultInfo txnResultInfo : txnResult) {
            log.info("txnPreWrite txnResultInfo :" + txnResultInfo);
            LockInfo lockInfo = txnResultInfo.getLocked();
            if (lockInfo != null) {
                // CheckTxnStatus
                log.info("txnPreWrite lockInfo :" + lockInfo);
                long current_ts = TransactionManager.nextTimestamp();
                TxnCheckStatus txnCheckStatus = TxnCheckStatus.builder().
                    isolationLevel(IsolationLevel.of(isolationLevel)).
                    primary_key(lockInfo.getPrimaryKey()).
                    lock_ts(lockInfo.getLock_ts()).
                    caller_start_ts(start_ts).
                    current_ts(current_ts).
                    build();
                TxnCheckStatusResult statusResponse = part.txnCheckTxnStatus(txnCheckStatus);
                log.info("txnPreWrite txnCheckStatus :" + statusResponse);
                TxnResultInfo resultInfo = statusResponse.getTxnResultInfo();
                // success
                if (resultInfo == null) {
                    long lockTtl = statusResponse.getLock_ttl();
                    long commitTs = statusResponse.getCommit_ts();
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
                            start_ts(lockInfo.getLock_ts()).
                            commit_ts(commitTs).
                            keys(Collections.singletonList(lockInfo.getKey())).
                            build();
                        TxnResolveLockResult txnResolveLockResult = part.txnResolveLock(resolveLockRequest);
                        log.info("txnResolveLockResponse:" + txnResolveLockResult);
                    } else if (lockTtl == 0 && commitTs == 0) {
                        // resolveLock store rollback
                        TxnResolveLock resolveLockRequest = TxnResolveLock.builder().
                            isolationLevel(IsolationLevel.of(isolationLevel)).
                            start_ts(lockInfo.getLock_ts()).
                            commit_ts(commitTs).
                            keys(Collections.singletonList(lockInfo.getKey())).
                            build();
                        TxnResolveLockResult txnResolveLockResult = part.txnResolveLock(resolveLockRequest);
                        log.info("txnResolveLockResponse:" + txnResolveLockResult);
                    }
                } else {
                    // 1、PrimaryMismatch  or  TxnNotFound
                    if (resultInfo.getPrimary_mismatch() != null) {
                        throw new PrimaryMismatchException(resultInfo.getPrimary_mismatch().toString());
                    } else if (resultInfo.getTxn_not_found() != null) {
                        throw new RuntimeException(resultInfo.getTxn_not_found().toString());
                    }
                }
            } else {
                WriteConflict writeConflict = txnResultInfo.getWrite_conflict();
                log.info("txnPreWrite writeConflict :" + writeConflict);
                if (writeConflict != null) {
                    throw new WriteConflictException(writeConflict.toString());
                }
            }
        }
    }

    public static <T> List<Set<T>> splitSetIntoSubsets(Set<T> set, int batchSize) {
        List<T> tempList = new ArrayList<>(set);
        List<Set<T>> subsets = new ArrayList<>();
        for (int i = 0; i < tempList.size(); i += batchSize) {
            subsets.add(new HashSet<>(tempList.subList(i, Math.min(i + batchSize, tempList.size()))));
        }
        return subsets;
    }

}
