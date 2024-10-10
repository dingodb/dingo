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

package io.dingodb.server.executor.schedule;

import io.dingodb.calcite.executor.ShowLocksExecutor;
import io.dingodb.cluster.ClusterService;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.tenant.TenantConstant;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.sdk.service.DocumentService;
import io.dingodb.sdk.service.IndexService;
import io.dingodb.sdk.service.LockService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.sdk.service.entity.common.IndexParameter;
import io.dingodb.sdk.service.entity.common.IndexType;
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.Region;
import io.dingodb.sdk.service.entity.common.RegionDefinition;
import io.dingodb.sdk.service.entity.coordinator.GcFlagType;
import io.dingodb.sdk.service.entity.coordinator.GetRegionMapRequest;
import io.dingodb.sdk.service.entity.coordinator.UpdateGCSafePointRequest;
import io.dingodb.sdk.service.entity.store.Action;
import io.dingodb.sdk.service.entity.store.LockInfo;
import io.dingodb.sdk.service.entity.store.TxnCheckTxnStatusRequest;
import io.dingodb.sdk.service.entity.store.TxnCheckTxnStatusResponse;
import io.dingodb.sdk.service.entity.store.TxnPessimisticRollbackRequest;
import io.dingodb.sdk.service.entity.store.TxnResolveLockRequest;
import io.dingodb.sdk.service.entity.store.TxnScanLockRequest;
import io.dingodb.sdk.service.entity.store.TxnScanLockResponse;
import io.dingodb.sdk.service.entity.version.Kv;
import io.dingodb.sdk.service.entity.version.RangeRequest;
import io.dingodb.sdk.service.entity.version.RangeResponse;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.dingodb.common.mysql.InformationSchemaConstant.GLOBAL_VAR_PREFIX_BEGIN;
import static io.dingodb.sdk.common.utils.ByteArrayUtils.toHex;
import static io.dingodb.sdk.service.entity.store.Action.LockNotExistRollback;
import static io.dingodb.sdk.service.entity.store.Action.TTLExpirePessimisticRollback;
import static io.dingodb.sdk.service.entity.store.Action.TTLExpireRollback;
import static io.dingodb.sdk.service.entity.store.Op.Lock;
import static io.dingodb.store.proxy.Configuration.coordinatorSet;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public final class SafePointUpdateTask {

    private static final int PHYSICAL_SHIFT = 18;

    private static final String lockKeyStr =  "safe_point_update_" + TenantConstant.TENANT_ID;

    private static final String enableKeyStr = GLOBAL_VAR_PREFIX_BEGIN + "enable_safe_point_update";
    private static final String txnDurationKeyStr = GLOBAL_VAR_PREFIX_BEGIN + "txn_history_duration";

    private static final byte[] txnDurationKey = txnDurationKeyStr.getBytes(UTF_8);

    public static final RangeRequest enableKeyReq = RangeRequest.builder().key(enableKeyStr.getBytes(UTF_8)).build();

    private static final List<Action> pessimisticRollbackActions = Arrays.asList(
        LockNotExistRollback, TTLExpirePessimisticRollback, TTLExpireRollback
    );

    private static final AtomicBoolean running = new AtomicBoolean(false);

    private SafePointUpdateTask() {
    }

    public static void run() {
        Executors.execute(lockKeyStr, () -> {
            LockService lockService = new LockService(lockKeyStr, Configuration.coordinators());
            try {
                String value = DingoConfiguration.serverId() + "#" + DingoConfiguration.location();
                LockService.Lock lock = lockService.newLock(value);
                lock.lock();
                LogUtils.info(log, "Start safe point update task.");
                ScheduledFuture<?> future = Executors.scheduleWithFixedDelay(
                    lockKeyStr, SafePointUpdateTask::safePointUpdate, 1, 600, TimeUnit.SECONDS
                );
                lock.watchDestroy().thenRun(() -> {
                    future.cancel(true);
                    lockService.cancel();
                    run();
                });
            } catch (Exception e) {
                lockService.cancel();
                run();
            }
        });
    }

    private static void safePointUpdate() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            LogUtils.info(log, "Run safe point update task.");
            Set<Location> coordinators = coordinatorSet();
            long reqTs = tso();
            long safeTs = safeTs(coordinators, reqTs);
            List<Region> regions = Services.coordinatorService(coordinators).getRegionMap(
                reqTs, GetRegionMapRequest.builder().tenantId(TenantConstant.TENANT_ID).build()
            ).getRegionmap().getRegions();
            LogUtils.info(log, "Run safe point update task, current ts: {}, safe ts: {}", reqTs, safeTs);
            for (Region region : regions) {
                long regionId = region.getId();
                // skip non txn region
                if (region.getDefinition().getRange().getStartKey()[0] != 't') {
                    continue;
                }
                LogUtils.info(log, "Scan {} locks.", regionId);
                byte[] startKey = region.getDefinition().getRange().getStartKey();
                byte[] endKey = region.getDefinition().getRange().getEndKey();
                TxnScanLockResponse scanLockResponse;
                do {
                    LogUtils.info(log, "Scan {} locks range: [{}, {}).", regionId, toHex(startKey), toHex(endKey));
                    TxnScanLockRequest req = TxnScanLockRequest.builder()
                        .startKey(startKey).endKey(endKey).maxTs(safeTs).limit(1024).build();
                    if (isIndexRegion(region)) {
                        scanLockResponse = indexRegionService(regionId).txnScanLock(reqTs, req);
                    } else if (isDocumentRegion(region)) {
                        scanLockResponse = documentService(regionId).txnScanLock(reqTs, req);
                    } else {
                        scanLockResponse = storeRegionService(regionId).txnScanLock(reqTs, req);
                    }
                    if (scanLockResponse.getLocks() != null && !scanLockResponse.getLocks().isEmpty()) {
                        safeTs = resolveLock(safeTs, reqTs, scanLockResponse.getLocks(), coordinators, region);
                    }
                    if (scanLockResponse.isHasMore()) {
                        startKey = scanLockResponse.getEndKey();
                    } else {
                        break;
                    }
                } while (true);
            }

            LogUtils.info(log, "Update safe point to: {}", safeTs);
            if (!isDisable(reqTs)) {
                UpdateGCSafePointRequest.UpdateGCSafePointRequestBuilder<?, ?> builder = UpdateGCSafePointRequest.builder();
                // TODO
                builder.safePoint(0);
                if (TenantConstant.TENANT_ID == 0) {
                    builder.safePoint(safeTs - 1);
                } else {
                    builder.tenantSafePoints(Collections.singletonMap(TenantConstant.TENANT_ID, safeTs - 1));
                }
                Services.coordinatorService(coordinators).updateGCSafePoint(
                    reqTs, builder.build()
                );
            } else {
                LogUtils.info(log, "Safe point update task disabled, skip call coordinator.");
            }
        } catch (Exception e) {
            LogUtils.error(log, "Update safe point error, skip this run.", e);
            throw e;
        } finally {
            running.set(false);
        }
    }

    private static boolean isIndexRegion(Region region) {
        return Optional.ofNullable(region)
            .map(Region::getDefinition)
            .map(RegionDefinition::getIndexParameter)
            .map(IndexParameter::getIndexType)
            .filter($ -> $ == IndexType.INDEX_TYPE_VECTOR)
            .isPresent();
    }

    private static boolean isDocumentRegion(Region region) {
        return Optional.ofNullable(region)
            .map(Region::getDefinition)
            .map(RegionDefinition::getIndexParameter)
            .map(IndexParameter::getIndexType)
            .filter($ -> $ == IndexType.INDEX_TYPE_DOCUMENT)
            .isPresent();
    }

    private static boolean isDisable(long reqTs) {
       return Optional.of(Services.versionService(coordinatorSet()).kvRange(reqTs, enableKeyReq))
            .map(RangeResponse::getKvs)
            .filter($ -> !$.isEmpty())
            .map($ -> $.get(0))
            .map(Kv::getKv)
            .map(KeyValue::getValue)
            .map(String::new)
            .filter("1"::equals)
            .isAbsent();
    }

    private static StoreService storeRegionService(long regionId) {
        return Services.storeRegionService(Configuration.coordinatorSet(), regionId, 30);
    }

    private static IndexService indexRegionService(long regionId) {
        return Services.indexRegionService(Configuration.coordinatorSet(), regionId, 30);
    }

    private static DocumentService documentService(long regionId) {
        return Services.documentRegionService(Configuration.coordinatorSet(), regionId, 30);
    }

    private static long safeTs(Set<Location> coordinators, long requestId) {
        long safeTs;
        safeTs = Optional.ofNullable(Services.versionService(coordinators).kvRange(
                requestId, RangeRequest.builder().key(txnDurationKey).build()
            ).getKvs())
            .filter($ -> !$.isEmpty())
            .map($ -> $.get(0)).map(Kv::getKv)
            .map(KeyValue::getValue).map(String::new)
            .map(Long::parseLong)
            .map($ -> requestId - (TimeUnit.SECONDS.toMillis($) << PHYSICAL_SHIFT))
            .orElseGet(() -> requestId);
        long remoteMinStartTs = ClusterService.getDefault().getComputingLocations().stream()
            .filter($ -> !$.equals(DingoConfiguration.location()))
            .map($ -> ApiRegistry.getDefault().proxy(ShowLocksExecutor.Api.class, $))
            .mapToLong(ShowLocksExecutor.Api::getMinTs)
            .min().orElse(Long.MAX_VALUE);
        long localMinTs = TransactionManager.getMinTs();
        long minTxnTs = Math.min(remoteMinStartTs, localMinTs);

        return Math.min(minTxnTs, safeTs);
    }

    private static long tso() {
        return tsoService().tso();
    }

    private static TsoService tsoService() {
        return TsoService.getDefault();
    }

    private static boolean pessimisticRollback(
        long reqTs, LockInfo lock, Set<Location> coordinators, Region region
    ) {
        LogUtils.info(log, "Rollback pessimistic lock: {}, resolve ts: {}.", lock, reqTs);
        TxnPessimisticRollbackRequest req = TxnPessimisticRollbackRequest.builder()
            .startTs(lock.getLockTs())
            .forUpdateTs(lock.getForUpdateTs())
            .keys(Collections.singletonList(lock.getKey()))
            .build();
        if (isIndexRegion(region)) {
            return indexRegionService(region.getId()).txnPessimisticRollback(reqTs, req).getTxnResult() == null;
        }
        if (isDocumentRegion(region)) {
            return documentService(region.getId()).txnPessimisticRollback(reqTs, req).getTxnResult() == null;
        }
        return storeRegionService(region.getId()).txnPessimisticRollback(reqTs, req).getTxnResult() == null;
    }

    private static boolean resolve(
        long reqTs, LockInfo lock, long commitTs, Set<Location> coordinators, Region region
    ) {
        LogUtils.info(log, "Resolve lock: {}, resolve ts: {}, commit ts: {}.", lock, reqTs, commitTs);
        TxnResolveLockRequest req = TxnResolveLockRequest.builder()
            .startTs(lock.getLockTs())
            .commitTs(commitTs)
            .keys(Collections.singletonList(lock.getKey()))
            .build();
        if (isIndexRegion(region)) {
            return indexRegionService(region.getId()).txnResolveLock(reqTs, req).getTxnResult() == null;
        }
        if (isDocumentRegion(region)) {
            return documentService(region.getId()).txnResolveLock(reqTs, req).getTxnResult() == null;
        }
        return storeRegionService(region.getId()).txnResolveLock(reqTs, req).getTxnResult() == null;
    }

    private static TxnCheckTxnStatusResponse checkTxn(long safeTs, long reqTs, LockInfo lock) {
        LogUtils.info(log, "Check lock: {}, check ts: {}.", lock, reqTs);
        return Services.storeRegionService(coordinatorSet(), lock.getPrimaryLock(), 30).txnCheckTxnStatus(
            reqTs,
            TxnCheckTxnStatusRequest
                .builder()
                .callerStartTs(safeTs)
                .currentTs(safeTs)
                .lockTs(lock.getLockTs())
                .primaryKey(lock.getPrimaryLock())
                .build()
        );
    }

    private static boolean isPessimisticRollbackStatus(LockInfo lock, Action action) {
        return lock.getLockType() == Lock && lock.getForUpdateTs() != 0 && pessimisticRollbackActions.contains(action);
    }

    private static boolean isResolveLockStatus(TxnCheckTxnStatusResponse res) {
        return res.getCommitTs() > 0 || (res.getLockTtl() == 0 && res.getCommitTs() == 0);
    }

    private static long resolveLock(
        long safeTs, long reqTs, List<LockInfo> locks, Set<Location> coordinators, Region region
    ) {
        long result = safeTs;
        for (LockInfo lock : locks) {
            TxnCheckTxnStatusResponse checkTxnRes = checkTxn(safeTs, reqTs, lock);
            if (checkTxnRes.getTxnResult() == null) {
                if (isPessimisticRollbackStatus(lock, checkTxnRes.getAction())) {
                    if (!pessimisticRollback(reqTs, lock, coordinators, region)) {
                        result = min(result, lock.getLockTs());
                    }
                } else if (isResolveLockStatus(checkTxnRes)) {
                    if (!resolve(reqTs, lock, checkTxnRes.getCommitTs(), coordinators, region)) {
                        result = min(result, lock.getLockTs());
                    }
                } else {
                    result = min(result, lock.getLockTs());
                }
            } else {
                result = min(result, lock.getLockTs());
            }
        }
        return result;
    }

}
