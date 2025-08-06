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

package io.dingodb.store.proxy.common;

import io.dingodb.calcite.executor.ShowLocksExecutor;
import io.dingodb.cluster.ClusterService;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.Tenant;
import io.dingodb.common.session.Session;
import io.dingodb.common.session.SessionUtil;
import io.dingodb.common.tenant.TenantConstant;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Pair;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.sdk.service.CoordinatorService;
import io.dingodb.sdk.service.DocumentService;
import io.dingodb.sdk.service.IndexService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.sdk.service.entity.common.IndexParameter;
import io.dingodb.sdk.service.entity.common.IndexType;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.Region;
import io.dingodb.sdk.service.entity.common.RegionDefinition;
import io.dingodb.sdk.service.entity.coordinator.DropRegionRequest;
import io.dingodb.sdk.service.entity.coordinator.GetGCSafePointRequest;
import io.dingodb.sdk.service.entity.coordinator.GetGCSafePointResponse;
import io.dingodb.sdk.service.entity.coordinator.GetRegionMapRequest;
import io.dingodb.sdk.service.entity.coordinator.UpdateGCSafePointRequest;
import io.dingodb.sdk.service.entity.meta.DeleteAutoIncrementRequest;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.EntityType;
import io.dingodb.sdk.service.entity.store.Action;
import io.dingodb.sdk.service.entity.store.LockInfo;
import io.dingodb.sdk.service.entity.store.TxnCheckTxnStatusRequest;
import io.dingodb.sdk.service.entity.store.TxnCheckTxnStatusResponse;
import io.dingodb.sdk.service.entity.store.TxnPessimisticRollbackRequest;
import io.dingodb.sdk.service.entity.store.TxnResolveLockRequest;
import io.dingodb.sdk.service.entity.store.TxnScanLockRequest;
import io.dingodb.sdk.service.entity.store.TxnScanLockResponse;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.checkstatus.AsyncResolveData;
import io.dingodb.store.api.transaction.exception.NonAsyncCommitLockException;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.transaction.api.GcApi;
import io.dingodb.transaction.api.GcObj;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static io.dingodb.common.util.NameCaseUtils.convertSql;
import static io.dingodb.sdk.common.utils.ByteArrayUtils.toHex;
import static io.dingodb.sdk.service.entity.store.Action.LockNotExistRollback;
import static io.dingodb.sdk.service.entity.store.Action.TTLExpirePessimisticRollback;
import static io.dingodb.sdk.service.entity.store.Action.TTLExpireRollback;
import static io.dingodb.sdk.service.entity.store.Op.Lock;
import static io.dingodb.store.proxy.Configuration.coordinatorSet;
import static io.dingodb.store.utils.ResolveLockUtil.checkSecondaryAllLocks;
import static io.dingodb.store.utils.ResolveLockUtil.resolveAsyncResolveData;
import static java.lang.Math.min;

@Slf4j
public class Gc {

    private static final List<Action> pessimisticRollbackActions = Arrays.asList(
        LockNotExistRollback, TTLExpirePessimisticRollback, TTLExpireRollback
    );

    public static Pair<String, Long> safePointUpdate() {
        LogUtils.info(log, "safe point update task start.");
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        if (!infoSchemaService.prepare()) {
            Utils.sleep(1000);
            return safePointUpdate();
        }
        if (!GcApi.running.compareAndSet(false, true)) {
            return new Pair<>(GcStatus.RUNNING.toString(), 0L);
        }
        try {
            LogUtils.info(log, "Run safe point update task.");
            Set<Location> coordinators = coordinatorSet();
            long reqTs = tso();
            long safeTs = safeTs(getTxnDurationSafeTs(reqTs));
            List<Region> regions = getRegions(coordinators, reqTs);
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
                    LogUtils.info(log, "Scan {} locks range: [{}, {}).",
                        regionId, toHex(startKey), toHex(endKey));
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
                }
                while (true);
            }

            LogUtils.info(log, "Update safe point to safeTs: {}, reqTs: {}", safeTs, reqTs);
            if (enable(reqTs)) {
                UpdateGCSafePointRequest.UpdateGCSafePointRequestBuilder<?, ?> builder
                    = UpdateGCSafePointRequest.builder();
                builder.safePoint(0);
                if (TenantConstant.TENANT_ID == 0) {
                    builder.safePoint(safeTs - 1);
                } else {
                    builder.tenantSafePoints(Collections.singletonMap(TenantConstant.TENANT_ID, safeTs - 1));
                }
                UpdateGCSafePointRequest request = builder.build();
                Services.coordinatorService(coordinators).updateGCSafePoint(
                    reqTs, request
                );
            } else {
                LogUtils.info(log, "Safe point update task disabled, skip call coordinator.");
            }
            return new Pair<>(GcStatus.FINISH.toString(), safeTs - 1);
        } catch (Exception e) {
            LogUtils.error(log, "Update safe point error, skip this run.", e);
            throw e;
        } finally {
            GcApi.running.set(false);
        }
    }

    private static List<Region> getRegions(Set<Location> coordinators, long reqTs) {
        Integer retry = Optional.mapOrGet(
            DingoConfiguration.instance().find("retry", int.class),
            __ -> __,
            () -> 30
        );
        while (retry-- > 0) {
            try {
                return Services.coordinatorService(coordinators).getRegionMap(
                    reqTs, GetRegionMapRequest.builder().tenantId(TenantConstant.TENANT_ID).build()
                ).getRegionmap().getRegions();
            } catch (Exception e) {
                LogUtils.warn(log, "Get region map failed, retry times: {}", retry, e);
            }
        }
        throw new RuntimeException("Tenant id:" + TenantConstant.TENANT_ID + " get region map failed.");
    }

    private static List<Region> getRegions(Set<Location> coordinators, long reqTs, Tenant tenant) {
        Integer retry = Optional.mapOrGet(
            DingoConfiguration.instance().find("retry", int.class),
            __ -> __,
            () -> 30
        );
        while (retry-- > 0) {
            try {
                return Services.coordinatorService(coordinators).getRegionMap(
                    reqTs, GetRegionMapRequest.builder().tenantId(tenant.getId()).build()
                ).getRegionmap().getRegions();
            } catch (Exception e) {
                LogUtils.warn(log, "Get region map failed, retry times: {}", retry, e);
            }
        }
        throw new RuntimeException("Tenant id:" + tenant.getId() + " get region map failed.");
    }

    public static GcObj startBackUpSafeByPoint(long point, long latestTso) {
        LogUtils.info(log, "back up safe point update task start. latestTso:{}, to point:{}", latestTso, point);
        if (!GcApi.running.compareAndSet(false, true)) {
            return GcObj.builder()
                .status(GcStatus.GC_TASK_RUNNING.toString())
                .safePoint(0L)
                .resolveLockSafePoint(0L)
                .build();
        }
        try {
            LogUtils.info(log, "Run back up safe point update task.");
            Set<Location> coordinators = coordinatorSet();
            long safeTs = safeTs(point);
            List<Region> regions = getRegions(coordinators, latestTso);
            LogUtils.info(log, "Run back up safe point update task, current ts: {}, safe ts: {}",
                latestTso, safeTs);
            for (Region region : regions) {
                long regionId = region.getId();
                // skip non txn region
                if (region.getDefinition().getRange().getStartKey()[0] != 't') {
                    continue;
                }
                LogUtils.info(log, "Back up scan {} locks.", regionId);
                byte[] startKey = region.getDefinition().getRange().getStartKey();
                byte[] endKey = region.getDefinition().getRange().getEndKey();
                TxnScanLockResponse scanLockResponse;
                do {
                    LogUtils.info(log, "Back up scan {} locks range: [{}, {}).", regionId,
                        toHex(startKey), toHex(endKey));
                    TxnScanLockRequest req = TxnScanLockRequest.builder()
                        .startKey(startKey).endKey(endKey).maxTs(safeTs).limit(1024).build();
                    if (isIndexRegion(region)) {
                        scanLockResponse = indexRegionService(regionId).txnScanLock(latestTso, req);
                    } else if (isDocumentRegion(region)) {
                        scanLockResponse = documentService(regionId).txnScanLock(latestTso, req);
                    } else {
                        scanLockResponse = storeRegionService(regionId).txnScanLock(latestTso, req);
                    }
                    if (scanLockResponse.getLocks() != null && !scanLockResponse.getLocks().isEmpty()) {
                        safeTs = resolveLock(safeTs, latestTso, scanLockResponse.getLocks(), coordinators, region);
                    }
                    if (scanLockResponse.isHasMore()) {
                        startKey = scanLockResponse.getEndKey();
                    } else {
                        break;
                    }
                }
                while (true);
            }

            safeTs = safeTs - 1;
            LogUtils.info(log, "Back up Update safe point to safeTs: {}, latestTso: {}", safeTs, latestTso);

            GetGCSafePointRequest.GetGCSafePointRequestBuilder <?, ?> getBuilder = GetGCSafePointRequest.builder();
            long safePoint;
            if (TenantConstant.TENANT_ID == 0) {
                getBuilder.getAllTenant(false);
                GetGCSafePointRequest getGCSafePointRequest = getBuilder.build();
                GetGCSafePointResponse gcSafePoint = Services.coordinatorService(coordinators).getGCSafePoint(
                    latestTso, getGCSafePointRequest
                );
                safePoint = gcSafePoint.getSafePoint();

                if (safePoint > safeTs) {
                    throw new RuntimeException("gcSafePoint: " + safePoint + " is greater than resolveLockSafePoint: " + safeTs);
                }
                if (gcSafePoint.getResolveLockSafePoint() > safeTs) {
                    throw new RuntimeException("The currently calculated resolveLockSafePoint: " + safeTs +
                        " is less than coordinator's resolveLockSafePoint: " + gcSafePoint.getResolveLockSafePoint());
                }
            } else {
                getBuilder.getAllTenant(true);
                GetGCSafePointRequest getGCSafePointRequest = getBuilder.build();
                GetGCSafePointResponse gcSafePoint = Services.coordinatorService(coordinators).getGCSafePoint(
                    latestTso, getGCSafePointRequest
                );
                Map<Long, Long> tenantSafePoints = gcSafePoint.getTenantSafePoints();
                safePoint = tenantSafePoints.get(TenantConstant.TENANT_ID);
                if (safePoint > safeTs) {
                    throw new RuntimeException("gcSafePoint: " + safePoint + " is greater than resolveLockSafePoint: " + safeTs);
                }
                Map<Long, Long> tenantResolveLockSafePoints = gcSafePoint.getTenantResolveLockSafePoints();
                long resolveLockSafePoint = tenantResolveLockSafePoints.get(TenantConstant.TENANT_ID);
                if (resolveLockSafePoint > safeTs) {
                    throw new RuntimeException("The currently calculated resolveLockSafePoint: " + safeTs +
                        " is less than coordinator's resolveLockSafePoint: " + resolveLockSafePoint);
                }
            }

            UpdateGCSafePointRequest.UpdateGCSafePointRequestBuilder<?, ?> builder
                = UpdateGCSafePointRequest.builder();
            if (TenantConstant.TENANT_ID == 0) {
                builder.resolveLockSafePoint(safeTs);
            } else {
                builder.tenantResolveLockSafePoints(Collections.singletonMap(TenantConstant.TENANT_ID, safeTs));
            }
            UpdateGCSafePointRequest request = builder.build();
            Services.coordinatorService(coordinators).updateGCSafePoint(
                latestTso, request
            );
            return GcObj.builder()
                .status(GcStatus.FINISH.toString())
                .safePoint(safePoint)
                .resolveLockSafePoint(safeTs)
                .build();
        } catch (Exception e) {
            LogUtils.error(log, "Back up update safe point error, skip this run.", e);
            throw e;
        } finally {
            GcApi.running.set(false);
        }
    }

    public static List<GcObj> startTenantsBackUpSafeByPoint(long point, long latestTso) {
        if (TenantConstant.TENANT_ID != 0L) {
            throw new RuntimeException("The current tenant " +TenantConstant.TENANT_ID+ " does not have permission");
        }
        if (!GcApi.running.compareAndSet(false, true)) {
            GcObj gcObj = GcObj.builder()
                .status(GcStatus.GC_TASK_RUNNING.toString())
                .safePoint(0L)
                .resolveLockSafePoint(0L)
                .build();
            return Collections.singletonList(gcObj);
        }
        LogUtils.info(log, "Tenants back up safe point update task start. " +
            "latestTso:{}, to point:{}", latestTso, point);
        List<GcObj> gcObs = new ArrayList<>();
        try {
            LogUtils.info(log, "Run tenants back up safe point update task.");
            Set<Location> coordinators = coordinatorSet();
            List<Object> tenants = InfoSchemaService.root().listTenant()
                .stream().
                filter( t -> ((Tenant)t).isDelete())
                .toList();
            for (Object tenant: tenants) {
                long safeTs = point;
                Tenant te = (Tenant) tenant;
                List<Region> regions = getRegions(coordinators, latestTso, te);
                for (Region region : regions) {
                    long regionId = region.getId();
                    // skip non txn region
                    if (region.getDefinition().getRange().getStartKey()[0] != 't') {
                        continue;
                    }
                    LogUtils.info(log, "Back up Tenant id {}  scan {} locks.", te.getId(), regionId);
                    byte[] startKey = region.getDefinition().getRange().getStartKey();
                    byte[] endKey = region.getDefinition().getRange().getEndKey();
                    TxnScanLockResponse scanLockResponse;
                    do {
                        LogUtils.info(log, "Back up Tenant id {} scan {} locks range: [{}, {}).", te.getId(),
                            regionId, toHex(startKey), toHex(endKey));
                        TxnScanLockRequest req = TxnScanLockRequest.builder()
                            .startKey(startKey).endKey(endKey).maxTs(safeTs).limit(1024).build();
                        if (isIndexRegion(region)) {
                            scanLockResponse = indexRegionService(regionId).txnScanLock(latestTso, req);
                        } else if (isDocumentRegion(region)) {
                            scanLockResponse = documentService(regionId).txnScanLock(latestTso, req);
                        } else {
                            scanLockResponse = storeRegionService(regionId).txnScanLock(latestTso, req);
                        }
                        if (scanLockResponse.getLocks() != null && !scanLockResponse.getLocks().isEmpty()) {
                            safeTs = resolveLock(safeTs, latestTso, scanLockResponse.getLocks(), coordinators, region);
                        }
                        if (scanLockResponse.isHasMore()) {
                            startKey = scanLockResponse.getEndKey();
                        } else {
                            break;
                        }
                    }
                    while (true);
                }
                safeTs = safeTs - 1;
                LogUtils.info(log, "Back up Tenant id {} Update safe point to safeTs: {}, latestTso: {}",
                    te.getId(), safeTs, latestTso);

                GetGCSafePointRequest.GetGCSafePointRequestBuilder <?, ?> getBuilder = GetGCSafePointRequest.builder();
                long safePoint;
                getBuilder.getAllTenant(true);
                GetGCSafePointRequest getGCSafePointRequest = getBuilder.build();
                GetGCSafePointResponse gcSafePoint = Services.coordinatorService(coordinators).getGCSafePoint(
                    latestTso, getGCSafePointRequest
                );
                Map<Long, Long> tenantSafePoints = gcSafePoint.getTenantSafePoints();
                safePoint = tenantSafePoints.get(te.getId());
                if (safePoint > safeTs) {
                    throw new RuntimeException("Tenant id: " + te.getId() + " gcSafePoint: "
                        + safePoint + " is greater than resolveLockSafePoint: " + safeTs);
                }
                Map<Long, Long> tenantResolveLockSafePoints = gcSafePoint.getTenantResolveLockSafePoints();
                long resolveLockSafePoint = tenantResolveLockSafePoints.get(te.getId());
                if (resolveLockSafePoint > safeTs) {
                    throw new RuntimeException("The currently calculated tenant id: " + te.getId() +
                        " resolveLockSafePoint: " + safeTs + " is less than coordinator's resolveLockSafePoint: "
                        + resolveLockSafePoint);
                }

                UpdateGCSafePointRequest.UpdateGCSafePointRequestBuilder<?, ?> builder
                    = UpdateGCSafePointRequest.builder();
                builder.tenantResolveLockSafePoints(Collections.singletonMap(te.getId(), safeTs));
                UpdateGCSafePointRequest request = builder.build();
                Services.coordinatorService(coordinators).updateGCSafePoint(
                    latestTso, request
                );
                gcObs.add(
                    GcObj.builder()
                        .tenant(String.valueOf(te.getId()))
                        .status(GcStatus.FINISH.toString())
                        .safePoint(safePoint)
                        .resolveLockSafePoint(safeTs)
                    .build()
                );
            }
            return gcObs;
        } catch (Exception e) {
            LogUtils.error(log, "Tenants back up update safe point error, skip this run.", e);
            throw e;
        } finally {
            GcApi.running.set(false);
        }
    }

    private static long getTxnDurationSafeTs(long requestId) {
        Map<String, String> globalVariablesMap = InfoSchemaService.root().getGlobalVariables();
        String durationStr = globalVariablesMap.get(GcApi.txnDurationKeyStr);
        if (durationStr == null) {
            return requestId;
        }
        long duration = Long.parseLong(durationStr);
        return requestId - (TimeUnit.SECONDS.toMillis(duration) << GcApi.PHYSICAL_SHIFT);
    }

    private static long safeTs(long safeTs) {
        Integer retry = Optional.mapOrGet(
            DingoConfiguration.instance().find("retry", int.class),
            __ -> __,
            () -> 30
        );
        long remoteMinStartTs = Long.MAX_VALUE;
        while (retry-- > 0) {
            try {
                LongStream longStream = ClusterService.getDefault().getComputingLocations().stream()
                    .filter($ -> !$.equals(DingoConfiguration.location()))
                    .map($ -> ApiRegistry.getDefault().proxy(ShowLocksExecutor.Api.class, $))
                    .mapToLong(ShowLocksExecutor.Api::getMinTs);
                remoteMinStartTs = longStream.min().orElse(Long.MAX_VALUE);
                break;
            } catch (Exception e) {
                LogUtils.warn(log, "Cross node get remote min start ts failed, retry times: {}", retry, e);
            }
        }
        long localMinTs = TransactionManager.getMinTs();
        long minTxnTs = Math.min(remoteMinStartTs, localMinTs);

        return Math.min(minTxnTs, safeTs);
    }

    private static TsoService tsoService() {
        return TsoService.getDefault();
    }

    private static long tso() {
        return tsoService().tso();
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

    private static boolean enable(long reqTs) {
        Map<String,String> globalVariables = InfoSchemaService.root().getGlobalVariables();
        String enableGc = globalVariables.get(GcApi.enableKeyStr);
        return "1".equalsIgnoreCase(enableGc);
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


    private static TxnCheckTxnStatusResponse checkTxn(long safeTs, long reqTs, LockInfo lock, boolean forceSyncCommit) {
        LogUtils.info(log, "Check lock: {}, check ts: {}, forceSyncCommit:{}.", lock, reqTs, forceSyncCommit);
        return Services.storeRegionService(coordinatorSet(), lock.getPrimaryLock(), 30).txnCheckTxnStatus(
            reqTs,
            TxnCheckTxnStatusRequest
                .builder()
                .callerStartTs(safeTs)
                .currentTs(safeTs)
                .lockTs(lock.getLockTs())
                .primaryKey(lock.getPrimaryLock())
                .forceSyncCommit(forceSyncCommit)
                .build()
        );
    }

    private static boolean isPessimisticRollbackStatus(LockInfo lock, Action action) {
        return lock.getLockType() == Lock && lock.getForUpdateTs() != 0 && pessimisticRollbackActions.contains(action);
    }

    private static boolean isResolveLockStatus(TxnCheckTxnStatusResponse res) {
        return res.getCommitTs() > 0 || (res.getLockTtl() == 0 && res.getCommitTs() == 0);
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

    private static long resolveLock(
        long safeTs, long reqTs, List<LockInfo> locks, Set<Location> coordinators, Region region
    ) {
        long result = safeTs;
        boolean forceSyncCommit = false;
        for (LockInfo lock : locks) {
            try {
                result = resolveLockConflict(
                    safeTs,
                    reqTs,
                    coordinators,
                    region,
                    result,
                    forceSyncCommit,
                    lock
                );
            } catch (NonAsyncCommitLockException e) {
                result = resolveLockConflict(
                    safeTs,
                    reqTs,
                    coordinators,
                    region,
                    result,
                    true,
                    lock
                );
            }
        }
        return result;
    }

    private static long resolveLockConflict(long safeTs, long reqTs, Set<Location> coordinators,
                                  Region region, long result,
                                  boolean forceSyncCommit, LockInfo lock) {
        TxnCheckTxnStatusResponse checkTxnRes = checkTxn(safeTs, reqTs, lock, forceSyncCommit);
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
        } else if (checkTxnRes.getTxnResult() != null && checkTxnRes.getTxnResult().getLocked() != null) {
            lock = checkTxnRes.getTxnResult().getLocked();
            if (lock.isUseAsyncCommit() && !forceSyncCommit) {
                long lockTtl = lock.getLockTtl();
                if (lockTtl > 0 && !tsoService().IsExpired(lockTtl)) {
                    LogUtils.info(log, "reqTs:{}, lockTs:{} useAsyncCommit lockTtl not IsExpired, " +
                        "lockTtl:{}", reqTs, lock.getLockTs(), lockTtl);
                } else {
                    result = resolveAsyncCommitLock(reqTs, result, lock);
                }
            }
        } else {
            result = min(result, lock.getLockTs());
        }
        return result;
    }

    private static long resolveAsyncCommitLock(long reqTs, long result, LockInfo lock) {
        LogUtils.info(log, "reqTs:{}, lockTs:{} useAsyncCommit check, minCommitTs:{}",
            reqTs, lock.getLockTs(), lock.getMinCommitTs());
        try {
            List<byte[]> secondaries = lock.getSecondaries();
            AsyncResolveData asyncResolveData = AsyncResolveData.builder()
                .missingLock(false)
                .commitTs(lock.getMinCommitTs())
                .keys(new HashSet<>(secondaries))
                .build();
            // checkSecondaryLocks and asyncResolveData add keys
            checkSecondaryAllLocks(
                IsolationLevel.SnapshotIsolation.getCode(),
                reqTs,
                lock,
                secondaries,
                asyncResolveData
            );
            asyncResolveData.getKeys().add(lock.getPrimaryLock());
            LogUtils.info(log, "reqTs:{}, asyncResolveData:{}", reqTs, asyncResolveData);
            Integer retry = Optional.mapOrGet(
                DingoConfiguration.instance().find("retry", int.class),
                __ -> __,
                () -> 30
            );
            // resolveAsyncResolveData
            resolveAsyncResolveData(
                IsolationLevel.SnapshotIsolation.getCode(),
                reqTs,
                "",
                asyncResolveData,
                retry,
                lock
            );
            result = min(result, lock.getLockTs());
        } catch (NonAsyncCommitLockException e) {
            throw e;
        } catch (Exception e) {
            LogUtils.error(log, "reqTs:" + reqTs + ", error:" + e.getMessage(), e);
        }
        return result;
    }

    public static void gcDeleteRegion() {
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        if (!infoSchemaService.prepare()) {
            return;
        }
        try {
            Set<Location> coordinators = coordinatorSet();
            GetGCSafePointRequest.GetGCSafePointRequestBuilder<?, ?> getBuilder = GetGCSafePointRequest.builder();
            GetGCSafePointRequest getGCSafePointRequest = getBuilder.build();
            GetGCSafePointResponse gcSafePoint = Services.coordinatorService(coordinators).getGCSafePoint(
                TsoService.getDefault().tso(), getGCSafePointRequest
            );
            if (gcSafePoint.isGcStop()) {
                return;
            }
        } catch (Exception e) {
            LogUtils.warn(log, "validator gc stop error:{}", e.getMessage(), e);
        }
        Map<String, String> globalVarMap = InfoSchemaService.root().getGlobalVariables();
        String jobGc = globalVarMap.getOrDefault("job_need_gc", "on");
        if ("off".equalsIgnoreCase(jobGc)) {
            return;
        }
        if (!enable(0)) {
            return;
        }
        long currentTime = System.currentTimeMillis();
        String gcLifeTimeStr = InfoSchemaService.root().getGlobalVariables().get("txn_history_duration");
        long gcLifeTime = Long.parseLong(gcLifeTimeStr);
        long safePointTs = currentTime - (gcLifeTime * 1000);
        long tso = TsoService.getDefault().tso(safePointTs);
        InfoSchemaService.root().putGlobalVariable("safepoint_ts", tso);
        LogUtils.info(log, "gcDeleteRegion tso:{}", tso);
        gcDeleteRange(tso);
    }

    private static void gcDeleteRange(long startTs) {
        String sql = "select region_id,start_key,end_key,job_id,ts, element_id, element_type"
            + " from mysql.gc_delete_range where ts<" + startTs;
        sql = convertSql(sql);
        LogUtils.info(log, "gcDeleteRange sql:{}", sql);
        Session session = SessionUtil.INSTANCE.getSession();
        try {
            CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
            List<Object[]> gcResults = session.executeQuery(sql);
            LogUtils.info(log, "gcDeleteRange result size: {}, safePointTs:{}",
                gcResults.size(), startTs);
            int gcResultSize = gcResults.size();
            AtomicInteger delDone = new AtomicInteger(0);
            gcResults.forEach(objects -> {
                // eleType: schema table table_auto index
                String eleType = (String) objects[6];
                long regionId = (long) objects[0];
                if ("SCHEMA".equalsIgnoreCase(eleType)) {
                    String eleId = (String) objects[5];
                    long schemaId = Long.parseLong(eleId);
                    InfoSchemaService.root().dropSchema(schemaId);
                    long jobId = (long) objects[3];
                    long ts = (long) objects[4];
                    gcDeleteDone(jobId, ts, regionId, "", "", eleId, eleType, false);
                    LogUtils.info(log, "gc schema meta, schemaId:{}", eleId);
                    return;
                }
                try {
                    coordinatorService.dropRegion(
                        tso(),
                        DropRegionRequest.builder().regionId(regionId).build()
                    );
                    LogUtils.info(log, "gcDeleteRange success, regionId:{}", regionId);
                    long jobId = (long) objects[3];
                    long ts = (long) objects[4];
                    String startKey = objects[1].toString();
                    String endKey = objects[2].toString();
                    String eleId = (String) objects[5];
                    dropTableMeta(eleId, jobId, session, eleType);
                    if (!gcDeleteDone(jobId, ts, regionId, startKey, endKey, eleId, eleType, true)) {
                        LogUtils.error(log, "remove gcDeleteTask failed, jobId:{}, eleId:{}, eleType:{}", jobId, eleId, eleType);
                    } else {
                        delDone.incrementAndGet();
                    }
                    if (eleType.endsWith("auto")) {
                        String[] eleIds = eleId.split("-");
                        DingoCommonId tableId = DingoCommonId.builder()
                            .parentEntityId(Long.parseLong(eleIds[0]))
                            .entityId(Long.parseLong(eleIds[1]))
                            .entityType(EntityType.ENTITY_TYPE_TABLE)
                            .build();
                        DeleteAutoIncrementRequest req = DeleteAutoIncrementRequest.builder()
                            .tableId(tableId)
                            .build();
                        io.dingodb.sdk.service.MetaService metaService
                            = Services.autoIncrementMetaService(Configuration.coordinatorSet());
                        metaService.deleteAutoIncrement(System.identityHashCode(req), req);
                        LogUtils.info(log, "delAutoInc success, tableId:{}", tableId);
                    }
                } catch (Exception e) {
                    LogUtils.error(log, "gcDeleteRange error, regionId:{}", regionId, e);
                }
            });
            LogUtils.info(log, "delete region done size:{}", delDone.get());
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        } finally {
            SessionUtil.INSTANCE.closeSession(session);
        }
    }

    static boolean dropTableMeta(String eleId, long jobId, Session session, String eleType) {
        if ("INDEX".equalsIgnoreCase(eleType) || "TABLE_part".equalsIgnoreCase(eleType)) {
            return true;
        }
        if (jobId > 0) {
            String sql = "select type from mysql.dingo_ddl_history where job_id=" + jobId;
            try {
                List<Object[]> res = session.executeQuery(sql);
                if (res.isEmpty()) {
                    LogUtils.error(log, "drop table meta get job type empty, jobId:{}", jobId);
                } else {
                    if (res.get(0).length >= 1) {
                        int type = (int) res.get(0)[0];
                        // truncate/drop
                        if (type != 3 && type != 11) {
                            LogUtils.info(log, "drop table meta skip, type:{},jobId:{}", type, jobId);
                            return true;
                        }
                    }
                }
            } catch (SQLException e) {
                LogUtils.error(log, "drop table meta get job type error:{}, jobId:{}",e.getMessage(), jobId, e);
            }
        }
        if (eleId == null) {
            return false;
        }
        String[] ids = eleId.split("-");
        if (ids.length < 2) {
            return false;
        }
        try {
            long schemaId = Long.parseLong(ids[0]);
            long tableId = Long.parseLong(ids[1]);
            MetaService metaService = MetaService.root();
            boolean res = metaService.dropTableMeta(schemaId, tableId);
            LogUtils.info(log, "drop table meta done,schemaId:{}, tableId:{}", schemaId, tableId);
            return res;
        } catch (Exception e) {
            LogUtils.error(log, "dropSchemaMeta error,eleId:{},reason:{}", eleId, e.getMessage(), e);
            return false;
        }
    }

    public static boolean gcDeleteDone(
        Object jobId,
        Object ts,
        long regionId,
        Object startKey,
        Object endKey,
        String eleId,
        String eleType,
        boolean markGcDone
    ) {
        LogUtils.info(log, "gcDeleteDone start, jobId:{}, regionId:{}", jobId, regionId);
        Session session = SessionUtil.INSTANCE.getSession();
        if (markGcDone) {
            String sql = "insert into mysql.gc_delete_range_done(job_id, region_id, ts, start_key, end_key, "
                + " element_id, element_type)"
                + " values(%d, %d, %d, %s, %s, %s, %s)";
            sql = convertSql(String.format(sql, jobId, regionId, ts, Utils.quoteForSql(startKey.toString()),
                Utils.quoteForSql(endKey.toString()), Utils.quoteForSql(eleId), Utils.quoteForSql(eleType)));
            try {
                session.setAutoCommit(false);
                session.executeUpdate(sql);
                session.commit();
            } catch (Exception e) {
                LogUtils.error(log, e.getMessage(), e);
                session.rollback();
            }
        }
        try {
            String removeSql = "delete from mysql.gc_delete_range where job_id=" + jobId;
            session.executeUpdate(convertSql(removeSql));
            session.commit();
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            session.rollback();
        } finally {
            SessionUtil.INSTANCE.closeSession(session);
        }
        LogUtils.info(log, "gc gcDeleteDone, regionId:{}, jobId:{}, markGcDone:{}", regionId, jobId, markGcDone);
        return true;
    }

}
