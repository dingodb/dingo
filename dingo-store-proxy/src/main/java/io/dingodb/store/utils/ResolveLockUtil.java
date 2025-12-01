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

package io.dingodb.store.utils;

import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.log.MdcUtils;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.sdk.service.entity.store.Action;
import io.dingodb.sdk.service.entity.store.LockInfo;
import io.dingodb.sdk.service.entity.store.Op;
import io.dingodb.sdk.service.entity.store.TxnCheckSecondaryLocksResponse;
import io.dingodb.sdk.service.entity.store.TxnCheckTxnStatusResponse;
import io.dingodb.sdk.service.entity.store.TxnResolveLockResponse;
import io.dingodb.sdk.service.entity.store.TxnResultInfo;
import io.dingodb.sdk.service.entity.store.WriteConflict;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.TxnVariables;
import io.dingodb.store.api.transaction.data.checkstatus.AsyncResolveData;
import io.dingodb.store.api.transaction.data.checkstatus.TxnCheckSecondaryLocks;
import io.dingodb.store.api.transaction.data.checkstatus.TxnCheckStatus;
import io.dingodb.store.api.transaction.data.resolvelock.ResolveLockStatus;
import io.dingodb.store.api.transaction.data.resolvelock.TxnResolveLock;
import io.dingodb.store.api.transaction.exception.NonAsyncCommitLockException;
import io.dingodb.store.api.transaction.exception.PrimaryMismatchException;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.store.proxy.common.transaction.LockResolveManager;
import io.dingodb.store.proxy.common.transaction.ResolveLocksOptions;
import io.dingodb.store.proxy.common.transaction.TxnStatus;
import io.dingodb.store.proxy.service.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;

@Slf4j
public final class ResolveLockUtil {
    private ResolveLockUtil() {

    }

    public static boolean shouldRetry(ResolveLockStatus status, long timeOut, int retryCount) {
        return (status == ResolveLockStatus.LOCK_TTL || status == ResolveLockStatus.TXN_NOT_FOUND)
            && timeOut > 0 && retryCount < TxnVariables.WaitFixNum;
    }

    public static void doRetryWait(int retryCount, String operation) {
        long waitTime = calculateWaitTime(retryCount);
        try {
            Thread.sleep(waitTime);
            LogUtils.info(log, "{} wait {} ms end.", operation, waitTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static long calculateWaitTime(int retryCount) {
        if (retryCount < TxnVariables.WaitFixNum) {
            return TxnVariables.WaitTime * retryCount;
        }
        return TxnVariables.WaitFixTime;
    }

    public static boolean isTxnNotFoundError(Exception e) {
        return e.getMessage() != null && e.getMessage().contains("TxnNotFound");
    }

    public static boolean isPessimisticLockExpireRetTTL (Exception e) {
        return e.getMessage() != null && e.getMessage().contains("txnExpireRetTTL");
    }

    public static void doBackoff(int retryCount, String operation) {
        long backoffTime = Math.min(100 * (1L << retryCount), 1000L);
        try {
            Thread.sleep(backoffTime);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
    }

    public static List<LockInfo> extractLockInfos(List<TxnResultInfo> txnResult, long startTs,
                                                   String funName, boolean isOptimistic) {
        List<LockInfo> lockInfos = new ArrayList<>();
        for (TxnResultInfo txnResultInfo: txnResult) {
            LockInfo lockInfo = txnResultInfo.getLocked();
            if (lockInfo == null) {
                WriteConflict writeConflict = txnResultInfo.getWriteConflict();
                LogUtils.info(log, "startTs:{}, {} writeConflict : {}", startTs, funName, writeConflict);
                if (writeConflict != null) {
                    //  write column exist and commit_ts > for_update_ts
                    if (funName.equalsIgnoreCase("txnPessimisticLock")) {
                        continue;
                    }
                    throw new WriteConflictException(writeConflict.toString(), writeConflict.getKey());
                }
            } else {
                if (lockInfo.getLockTs() > startTs && isOptimistic
                    && (lockInfo.getLockType() != Op.Lock && lockInfo.getForUpdateTs() == 0)) {
                    throw new WriteConflictException("startTs=" + startTs + ", primaryKey=null, conflictTs=" +
                        lockInfo.getLockTs() + ", key=" + Arrays.toString(lockInfo.getKey()) +
                        ", conflictCommitTs=0, reason=Optimistic, ext$=null", lockInfo.getKey());
                }
                lockInfos.add(lockInfo);
            }
        }
        return lockInfos;
    }

    public static TxnStatus getTxnStatusFromLock(LockInfo lock, long currentTS, ResolveLocksOptions opts) {
        int retryCount = 0;
        final int maxRetry = 60;

        while (retryCount < maxRetry) {
            try {
                return getTxnStatus(
                    lock.getLockTs(),
                    lock.getPrimaryLock(),
                    currentTS,
                    opts
                );
            } catch (Exception e) {
                LogUtils.error(log, "startTs: currentTS, " + e.getMessage(), e);
                if (isTxnNotFoundError(e)) {

                    if (TsoService.INSTANCE.untilExpired(lock.getLockTtl()) <= 0) {
                        LogUtils.warn(log, "Lock txn not found and expired, lock:{}", lock);
                        opts.setRollbackIfNotExist(true);
                        continue;
                    }

                    if (lock.getLockType() == Op.Lock && isPessimisticLockExpireRetTTL(e)) {
                        return TxnStatus.builder()
                            .ttl(lock.getLockTtl())
                            .action(Action.TTLExpirePessimisticRollback)
                            .build();
                    }
                }

                // Other errors are backed up.
                if (retryCount < maxRetry - 1) {
                    doBackoff(retryCount, "getTxnStatusFromLock");
                    retryCount++;
                    continue;
                }
                throw e;
            }
        }

        throw new RuntimeException("Failed to get txn status after retries");
    }

    public static TxnStatus getTxnStatus(long txnID, byte[] primaryKey, long currentTS, ResolveLocksOptions opts) {
        // Check cache
        TxnStatus cached = LockResolveManager.getResolved(txnID);
        if (cached != null) {
            LogUtils.info(log, "startTs:{}, {} from cache txnStatus: {}",
                opts.getCallerStartTS(), opts.getFunName(), cached);
            return cached;
        }

        TxnCheckStatus checkRequest = TxnCheckStatus.builder()
            .primaryKey(primaryKey)
            .lockTs(txnID)
            .callerStartTs(opts.getCallerStartTS())
            .currentTs(currentTS)
            .rollbackIfNotExist(opts.isRollbackIfNotExist())
            .forceSyncCommit(opts.isForceSyncCommit())
            .build();

        TxnCheckTxnStatusResponse response = txnCheckTxnStatus(checkRequest);
        LogUtils.info(log, "startTs:{}, {} txnCheckStatus : {}",
            opts.getCallerStartTS(), opts.getFunName(), response);
        LockInfo primaryLock;
        long lockTtl = response.getLockTtl();
        TxnStatus status = TxnStatus.builder()
            .action(response.getAction())
            .build();
        TxnResultInfo txnResult = response.getTxnResult();
        if (txnResult != null) {
            if (txnResult.getTxnNotFound() != null) {
                LogUtils.warn(log, "startTs:{}, {} txnNotFound : {}",
                    opts.getCallerStartTS(), opts.getFunName(), txnResult.getTxnNotFound().toString());
                throw new RuntimeException(txnResult.getTxnNotFound().toString());
            } else if (txnResult.getPrimaryMismatch() != null) {
                throw new PrimaryMismatchException(txnResult.getPrimaryMismatch().toString());
            } else if (txnResult.getWriteConflict() != null) {
                throw new WriteConflictException(txnResult.getWriteConflict().toString(),
                    txnResult.getWriteConflict().getKey());
            }
            primaryLock = txnResult.getLocked();
            if (primaryLock == null) {
                LogUtils.warn(log, "startTs:{}, {} txnResult primaryLock is null ",
                    opts.getCallerStartTS(), opts.getFunName());
            } else if (primaryLock.isUseAsyncCommit() && !opts.isForceSyncCommit()) {
                lockTtl = primaryLock.getLockTtl();
                if (!TsoService.INSTANCE.IsExpired(lockTtl)) {
                    status.setTtl(lockTtl);
                }
            } else if (lockTtl != 0) {
                status.setTtl(lockTtl);
            } else {
                status.setCommitTs(response.getCommitTs());
                // Cache cacheable state
                if (status.isStatusCacheable()) {
                    LockResolveManager.saveResolved(txnID, status);
                }
            }
        } else {
            primaryLock = response.getLockInfo();
            if (primaryLock != null && primaryLock.isUseAsyncCommit() && !opts.isForceSyncCommit()) {
                if (!TsoService.INSTANCE.IsExpired(lockTtl)) {
                    status.setTtl(lockTtl);
                }
            } else if (lockTtl != 0) {
                status.setTtl(lockTtl);
            } else {
                status.setCommitTs(response.getCommitTs());
                // Cache cacheable state
                if (status.isStatusCacheable()) {
                    LockResolveManager.saveResolved(txnID, status);
                }
            }
        }
        status.setPrimaryLock(primaryLock);

        return status;
    }

    public static TxnStatus resolveAsyncCommitLock(ResolveLocksOptions opts, LockInfo lockInfo,
                                                    TxnStatus status) {
        List<byte[]> secondaries = status.getPrimaryLock().getSecondaries();
        int isolationLevel = opts.getIsolationLevel();
        long callerStartTS = opts.getCallerStartTS();
        String funName = opts.getFunName();
        AsyncResolveData asyncResolveData = AsyncResolveData.builder()
            .missingLock(false)
            .commitTs(lockInfo.getMinCommitTs())
            .keys(new HashSet<>(secondaries))
            .build();
        // checkSecondaryLocks and asyncResolveData add keys
        checkSecondaryAllLocks(
            isolationLevel,
            callerStartTS,
            lockInfo,
            secondaries,
            asyncResolveData
        );
        status.setCommitTs(asyncResolveData.getCommitTs());
        if (status.isStatusCacheable()) {
            LockResolveManager.saveResolved(lockInfo.getLockTs(), status);
        }

        asyncResolveData.getKeys().add(lockInfo.getPrimaryLock());
        LogUtils.info(log, "callerStartTS:{}, asyncResolveData:{}", callerStartTS, asyncResolveData);
        Integer retry = io.dingodb.common.util.Optional.mapOrGet(
            DingoConfiguration.instance().find("retry", int.class),
            __ -> __,
            () -> 30
        );
        if (opts.isForRead()) {
            // Asynchronous unlock
            Executors.execute("async-resolve-lock" + callerStartTS, () -> {
                try {
                    MdcUtils.removeTxnId();
                    ResolveLockStatus resolveLockStatus = resolveAsyncResolveData(
                        isolationLevel,
                        callerStartTS,
                        funName,
                        asyncResolveData,
                        retry,
                        lockInfo
                    );
                    status.setResolveLockStatus(resolveLockStatus);
                    LogUtils.info(log, "Async resolveAsyncCommitLock end status:{}", resolveLockStatus);
                } catch (Exception e) {
                    LogUtils.error(log, "Async resolve lock failed, startTS:" + callerStartTS, e);
                }
            });
        } else {
            ResolveLockStatus resolveLockStatus = resolveAsyncResolveData(
                isolationLevel,
                callerStartTS,
                funName,
                asyncResolveData,
                retry,
                lockInfo
            );
            status.setResolveLockStatus(resolveLockStatus);
            LogUtils.info(log, "ResolveAsyncCommitLock end status:{}", resolveLockStatus);
        }
        return status;
    }

    public static TxnCheckTxnStatusResponse txnCheckTxnStatus(TxnCheckStatus txnCheckStatus) {
        long start = System.currentTimeMillis();
        byte[] primaryKey = txnCheckStatus.getPrimaryKey();
        StoreService storeService = Services.storeRegionService(Configuration.coordinatorSet(), primaryKey, 30);
        Integer retry = Optional.mapOrGet(
            DingoConfiguration.instance().find("retry", int.class),
            __ -> __,
            () -> 30
        );
        TxnCheckTxnStatusResponse txnCheckTxnStatusResponse = new TxnCheckTxnStatusResponse();
        while (retry-- > 0) {
            try {
                return storeService.txnCheckTxnStatus(
                        txnCheckStatus.getCallerStartTs(),
                    MAPPER.checkTxnTo(txnCheckStatus)
                );

            } catch (RegionSplitException e) {
                LogUtils.error(log, e.getMessage(), e);
                storeService = Services.storeRegionService(Configuration.coordinatorSet(), primaryKey, 30);
            }
        }
        long sub = System.currentTimeMillis() - start;
        DingoMetrics.timer("txnCheckTxnStatusRpc").update(sub, TimeUnit.MILLISECONDS);
        return txnCheckTxnStatusResponse;
    }

    public static void checkSecondaryAllLocks(int isolationLevel, long startTs, LockInfo lockInfo,
                                              List<byte[]> secondaries,
                                              AsyncResolveData asyncResolveData) {
        Integer retry = Optional.mapOrGet(
            DingoConfiguration.instance().find("retry", int.class),
            __ -> __,
            () -> 30
        );
        Map<Long, List<byte[]>> keyMaps = new HashMap<>();
        for (byte[] secondKey: secondaries) {
            long regionId = Services.findRegion(Configuration.coordinatorSet(), secondKey);
            keyMaps.computeIfAbsent(regionId, k -> new ArrayList<>()).add(secondKey);
        }
        for (Map.Entry<Long, List<byte[]>> entry : keyMaps.entrySet()) {
            Long regionId = entry.getKey();
            List<byte[]> secondKeys = entry.getValue();
            checkSecondaries(
                isolationLevel,
                startTs,
                lockInfo,
                asyncResolveData,
                regionId,
                secondKeys,
                retry
            );
        }
    }

    private static void checkSecondaries(int isolationLevel, long startTs, LockInfo lockInfo,
                                         AsyncResolveData asyncResolveData,
                                         Long regionId, List<byte[]> secondKeys, int retry) {
        retry--;
        TxnCheckSecondaryLocksResponse txnCheckSecondaryLocksResponse;
        try {
            txnCheckSecondaryLocksResponse =
                checkSecondaryLocks(
                    isolationLevel,
                    startTs,
                    regionId,
                    secondKeys,
                    lockInfo
                );
        } catch (RegionSplitException e) {
            LogUtils.error(log, e.getMessage(), e);
            if (retry < 0) {
                throw e;
            }
            Map<Long, List<byte[]>> keyMaps = new HashMap<>();
            for (byte[] secondKey: secondKeys) {
                regionId = Services.findRegion(Configuration.coordinatorSet(), secondKey);
                keyMaps.computeIfAbsent(regionId, k -> new ArrayList<>()).add(secondKey);
            }
            for (Map.Entry<Long, List<byte[]>> entry : keyMaps.entrySet()) {
                Long newRegionId = entry.getKey();
                List<byte[]> newSecondKeys = entry.getValue();
                checkSecondaries(
                    isolationLevel,
                    startTs,
                    lockInfo,
                    asyncResolveData,
                    newRegionId,
                    newSecondKeys,
                    retry
                );
            }
            return;
        }
        LogUtils.info(log,"startTs:{}, txnCheckSecondaryLocksResponse:{}",
            startTs, txnCheckSecondaryLocksResponse);
        long asyncCommitTs = txnCheckSecondaryLocksResponse.getCommitTs();
        List<LockInfo> locks = txnCheckSecondaryLocksResponse.getLocks();
        // Check locks to see if any have been committed or rolled back.
        if (locks == null || locks.size() < secondKeys.size()) {
            // A lock is missing - the transaction must either have been rolled back or committed.
            if (!asyncResolveData.isMissingLock()) {
                // commitTS == 0 => lock has been rolled back.
                if (asyncCommitTs != 0 && asyncCommitTs < asyncResolveData.getCommitTs()) {
                    throw new RuntimeException("commit TS must be greater or equal to "
                        + "min commit TS: commit ts: " + asyncCommitTs
                        + ", min commit ts: " + asyncResolveData.getCommitTs());
                }
                asyncResolveData.setCommitTs(asyncCommitTs);
            }
            asyncResolveData.setMissingLock(true);
            if (asyncResolveData.getCommitTs() != asyncCommitTs) {
                throw new RuntimeException("commit TS mismatch in async commit recovery:"
                    + asyncResolveData.getCommitTs() + " and " + asyncCommitTs);
            }
            // We do not need to resolve the remaining locks because Store will have resolved them as appropriate.
            return;
        }
        // Save all locks to be resolved.
        for (LockInfo asyncLockInfo: locks) {
            if (asyncLockInfo.getLockTs() != lockInfo.getLockTs()) {
                throw new RuntimeException("unexpected timestamp, expected:"
                    + lockInfo.getLockTs() + ", found: "
                    + asyncLockInfo.getLockTs());
            }
            if (!asyncLockInfo.isUseAsyncCommit()) {
                // nonAsyncCommitLock error
                LogUtils.info(log, "startTs: {}, asyncResolveLock nonAsyncCommitLock", startTs);
                throw new NonAsyncCommitLockException(startTs + " asyncResolveLock nonAsyncCommitLock error");
            }
            if (!asyncResolveData.isMissingLock() && asyncLockInfo.getMinCommitTs() > asyncResolveData.getCommitTs()) {
                asyncResolveData.setCommitTs(asyncLockInfo.getMinCommitTs());
            }
            asyncResolveData.getKeys().add(lockInfo.getKey());
        }
    }

    public static TxnCheckSecondaryLocksResponse checkSecondaryLocks(int isolationLevel, long startTs,
                                                                     Long regionId, List<byte[]> secondKeys,
                                                                     LockInfo lockInfo) {
        StoreService storeService = Services.storeRegionService(
            Configuration.coordinatorSet(),
            regionId,
            TransactionUtil.STORE_RETRY
        );
        TxnCheckSecondaryLocks txnCheckSecondaryLocks = TxnCheckSecondaryLocks.builder()
            .isolationLevel(IsolationLevel.of(isolationLevel))
            .startTs(lockInfo.getLockTs())
            .keys(secondKeys)
            .build();

        TxnCheckSecondaryLocksResponse txnCheckSecondaryLocksResponse =
            storeService.txnCheckSecondaryLocks(
                startTs,
                MAPPER.checkTxnSecondaryLocksTo(txnCheckSecondaryLocks)
            );
        return txnCheckSecondaryLocksResponse;
    }

    public static ResolveLockStatus resolveAsyncResolveData(int isolationLevel, long startTs, String funName,
                                                            AsyncResolveData asyncResolveData, int retry,
                                                            LockInfo lockInfo) {
        retry--;
        Map<Long, List<byte[]>> resolveKeyMaps = new HashMap<>();
        for (byte[] resolveKey: asyncResolveData.getKeys()) {
            long regionId = Services.findRegion(Configuration.coordinatorSet(), resolveKey);
            resolveKeyMaps.computeIfAbsent(regionId, k -> new ArrayList<>()).add(resolveKey);
        }
        for (Map.Entry<Long, List<byte[]>> entry : resolveKeyMaps.entrySet()) {
            Long regionId = entry.getKey();
            List<byte[]> keys = entry.getValue();
            try {
                TxnResolveLockResponse txnResolveLockRes = resolveRegionLocks(
                    isolationLevel,
                    asyncResolveData,
                    regionId,
                    keys,
                    lockInfo
                );
                LogUtils.info(log, "startTs:{}, {} txnResolveLockResponse: {}",
                    startTs, funName, txnResolveLockRes);
            } catch (RegionSplitException e) {
                LogUtils.error(log, e.getMessage(), e);
                if (retry < 0) {
                    throw e;
                }
                return resolveAsyncResolveData(
                    isolationLevel,
                    startTs,
                    funName,
                    asyncResolveData,
                    retry,
                    lockInfo
                );
            }
        }
        ResolveLockStatus resolveLockStatus;
        if (asyncResolveData.getCommitTs() > 0) {
            resolveLockStatus = ResolveLockStatus.COMMIT;
        } else {
            resolveLockStatus = ResolveLockStatus.ROLLBACK;
        }
        return resolveLockStatus;
    }

    public static TxnResolveLockResponse resolveRegionLocks(int isolationLevel,
                                                            AsyncResolveData asyncResolveData,
                                                            Long regionId,
                                                            List<byte[]> keys,
                                                            LockInfo lockInfo) {
        StoreService storeService = Services.storeRegionService(
            Configuration.coordinatorSet(),
            regionId,
            TransactionUtil.STORE_RETRY
        );
        TxnResolveLock resolveLockRequest = TxnResolveLock.builder()
            .isolationLevel(IsolationLevel.of(isolationLevel))
            .startTs(lockInfo.getLockTs())
            .commitTs(asyncResolveData.getCommitTs())
            .keys(keys)
            .build();
        TxnResolveLockResponse txnResolveLockRes =
            storeService.txnResolveLock(
                resolveLockRequest.getStartTs(),
                MAPPER.resolveTxnTo(resolveLockRequest)
            );
        return txnResolveLockRes;
    }
}
