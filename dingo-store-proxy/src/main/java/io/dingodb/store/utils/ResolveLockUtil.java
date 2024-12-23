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

import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.sdk.service.entity.store.LockInfo;
import io.dingodb.sdk.service.entity.store.TxnCheckSecondaryLocksResponse;
import io.dingodb.sdk.service.entity.store.TxnCheckTxnStatusResponse;
import io.dingodb.sdk.service.entity.store.TxnResolveLockResponse;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.checkstatus.AsyncResolveData;
import io.dingodb.store.api.transaction.data.checkstatus.TxnCheckSecondaryLocks;
import io.dingodb.store.api.transaction.data.checkstatus.TxnCheckStatus;
import io.dingodb.store.api.transaction.data.resolvelock.ResolveLockStatus;
import io.dingodb.store.api.transaction.data.resolvelock.TxnResolveLock;
import io.dingodb.store.api.transaction.exception.NonAsyncCommitLockException;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import io.dingodb.store.proxy.Configuration;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;

@Slf4j
public final class ResolveLockUtil {
    private ResolveLockUtil() {}

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
        long asyncCommitTs = txnCheckSecondaryLocksResponse.getCommitTs();
        List<LockInfo> locks = txnCheckSecondaryLocksResponse.getLocks();
        // Check locks to see if any have been committed or rolled back.
        if (locks.size() < secondKeys.size()) {
            // A lock is missing - the transaction must either have been rolled back or committed.
            if (!asyncResolveData.isMissingLock()) {
                // commitTS == 0 => lock has been rolled back.
                if (asyncCommitTs != 0 && asyncCommitTs < asyncResolveData.getCommitTs()) {
                    throw new RuntimeException("commit TS must be greater or equal to " +
                        "min commit TS: commit ts: " + asyncCommitTs +
                        ", min commit ts: " + asyncResolveData.getCommitTs());
                }
                asyncResolveData.setCommitTs(asyncCommitTs);
            }
            asyncResolveData.setMissingLock(true);
            if (asyncResolveData.getCommitTs() != asyncCommitTs) {
                throw new RuntimeException("commit TS mismatch in async commit recovery:" +
                    asyncResolveData.getCommitTs() + " and " + asyncCommitTs);
            }
            // We do not need to resolve the remaining locks because Store will have resolved them as appropriate.
            return;
        }
        // Save all locks to be resolved.
        for (LockInfo asyncLockInfo: locks) {
            if (asyncLockInfo.getLockTs() != lockInfo.getLockTs()) {
                throw new RuntimeException("unexpected timestamp, expected:" +
                    lockInfo.getLockTs() + ", found: " +
                    asyncLockInfo.getLockTs());
            }
            if (!asyncLockInfo.isUseAsyncCommit()) {
                // nonAsyncCommitLock error
                LogUtils.info(log, "startTs: {}, asyncResolveLock nonAsyncCommitLock", startTs);
                throw new NonAsyncCommitLockException(startTs + " asyncResolveLock nonAsyncCommitLock error");
            }
            if (!asyncResolveData.isMissingLock() &&
                asyncLockInfo.getMinCommitTs() > asyncResolveData.getCommitTs()) {
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
