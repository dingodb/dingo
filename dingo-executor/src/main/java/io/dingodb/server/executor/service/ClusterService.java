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

package io.dingodb.server.executor.service;

import com.google.auto.service.AutoService;
import io.dingodb.cluster.ClusterServiceProvider;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.tenant.TenantConstant;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.impl.JobImpl;
import io.dingodb.exec.impl.JobManagerImpl;
import io.dingodb.sdk.service.CoordinatorService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.common.Executor;
import io.dingodb.sdk.service.entity.common.ExecutorMap;
import io.dingodb.sdk.service.entity.common.ExecutorState;
import io.dingodb.sdk.service.entity.common.ExecutorUser;
import io.dingodb.sdk.service.entity.coordinator.ConfigCoordinatorRequest;
import io.dingodb.sdk.service.entity.coordinator.ConfigCoordinatorResponse;
import io.dingodb.sdk.service.entity.coordinator.ExecutorHeartbeatRequest;
import io.dingodb.sdk.service.entity.coordinator.GetExecutorMapRequest;
import io.dingodb.sdk.service.entity.coordinator.GetExecutorMapResponse;
import io.dingodb.sdk.service.entity.coordinator.GetGCSafePointRequest;
import io.dingodb.sdk.service.entity.coordinator.GetGCSafePointResponse;
import io.dingodb.sdk.service.entity.coordinator.GetRegionMapRequest;
import io.dingodb.sdk.service.entity.coordinator.GetStoreMapRequest;
import io.dingodb.sdk.service.entity.coordinator.GetStoreMapResponse;
import io.dingodb.server.executor.Configuration;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public final class ClusterService implements io.dingodb.cluster.ClusterService {

    public static final ClusterService DEFAULT_INSTANCE = new ClusterService();

    @AutoService(ClusterServiceProvider.class)
    public static final class Provider implements ClusterServiceProvider {
        @Override
        public io.dingodb.cluster.ClusterService get() {
            return DEFAULT_INSTANCE;
        }
    }

    private ClusterService() {
    }

    //
    // Cluster service.
    //
    public static final CoordinatorService coordinatorService = Services.coordinatorService(
        Services.parse(Configuration.coordinators())
    );

    private static Executor executor() {
        String leaderId = "";
        if (ExecutionEnvironment.INSTANCE.ddlOwner.get()) {
            leaderId = DingoConfiguration.serverId().toString();
        }
        return Executor.builder()
            .serverLocation(io.dingodb.sdk.service.entity.common.Location.builder()
                .host(DingoConfiguration.host())
                .port(DingoConfiguration.port())
                .build())
            .executorUser(ExecutorUser.builder()
                .user(Configuration.user())
                .keyring(Configuration.keyring())
                .build())
            .resourceTag(Configuration.resourceTag())
            .id(DingoConfiguration.serverId().toString())
            .clusterName("ExecutorCluster_" + TenantConstant.TENANT_ID)
            .leaderId(leaderId)
            .build();
    }

    private static ExecutorHeartbeatRequest executorHeartbeatRequest() {
        return ExecutorHeartbeatRequest.builder()
            .selfExecutormapEpoch(0)
            .executor(executor())
            .build();
    }

    @Override
    public List<Location> getComputingLocations() {
        return coordinatorService.getExecutorMap(
                TsoService.getDefault().cacheTso(),
                GetExecutorMapRequest.builder().clusterName("ExecutorCluster_" + TenantConstant.TENANT_ID).build()
            ).getExecutormap().getExecutors().stream()
            .filter($ -> $.getState() == ExecutorState.EXECUTOR_NORMAL)
            .map(io.dingodb.sdk.service.entity.common.Executor::getServerLocation)
            .map($ -> new Location($.getHost(), $.getPort()))
            .collect(Collectors.toList());
    }

    @Override
    public CommonId getServerId(Location location) {
        return Optional.ofNullable(coordinatorService.getExecutorMap(
                TsoService.getDefault().cacheTso(),
                GetExecutorMapRequest.builder().clusterName("ExecutorCluster_" + TenantConstant.TENANT_ID).build()
            )).map(GetExecutorMapResponse::getExecutormap)
            .map(ExecutorMap::getExecutors)
            .flatMap(executors -> executors.stream()
                .filter($ -> location.url().equals(url($.getServerLocation())))
                .findAny()
                .map(Executor::getId)
                .map(CommonId::parse)
            ).orElse(null);
    }

    @Override
    public Location getLocation(CommonId serverId) {
        return Optional.ofNullable(coordinatorService.getExecutorMap(
                TsoService.getDefault().cacheTso(),
                GetExecutorMapRequest.builder().clusterName("ExecutorCluster_" + TenantConstant.TENANT_ID).build()
            )).map(GetExecutorMapResponse::getExecutormap)
            .map(ExecutorMap::getExecutors)
            .flatMap(executors -> executors.stream()
                .filter($ -> CommonId.parse($.getId()).equals(serverId))
                .findAny()
                .map(Executor::getServerLocation)
                .map(this::url)
                .map(Location::parseUrl)
            ).orElse(null);
    }

    @Override
    public List<io.dingodb.common.Executor> getExecutors() {
        return coordinatorService.getExecutorMap(
            TsoService.getDefault().cacheTso(),
            GetExecutorMapRequest.builder().clusterName("ExecutorCluster_" + TenantConstant.TENANT_ID).build()
        ).getExecutormap().getExecutors().stream()
            .map(e -> io.dingodb.common.Executor.builder()
                .id(e.getId())
                .host(e.getServerLocation().getHost())
                .port(e.getServerLocation().getPort())
                .state(e.getState().name())
                .build())
            .collect(Collectors.toList());
    }

    @Override
    public int getStoreMap() {
        return coordinatorService.getStoreMap(
            TsoService.getDefault().cacheTso(), GetStoreMapRequest.builder().build()
        ).getStoremap().getStores().size();
    }

    @Override
    public int getLocations() {
        return coordinatorService.getStoreMap(
            TsoService.getDefault().cacheTso(), GetStoreMapRequest.builder().build()
        ).getStoremap().getStores().stream().map(s -> s.getRaftLocation().getHost()).collect(Collectors.toSet()).size();
    }

    @Override
    public void configCoordinator(boolean isReadOnly, String reason) {
        ConfigCoordinatorResponse configCoordinatorResponse = coordinatorService.configCoordinator(
            TsoService.getDefault().cacheTso(),
            ConfigCoordinatorRequest.builder()
                .isForceReadOnly(isReadOnly)
                .setForceReadOnly(true)
                .forceReadOnlyReason(reason)
                .build()
        );
    }

    @Override
    public List<Object[]> getStoreNodes() {
        GetStoreMapResponse response = coordinatorService.getStoreMap(
            TsoService.getDefault().cacheTso(), GetStoreMapRequest.builder().build()
        );
        if (response.getStoremap() == null || response.getStoremap().getStores() == null) {
            return new ArrayList<>();
        }
        return response.getStoremap().getStores().stream()
            .map(s -> new Object[] {
                s.getId(),
                s.getRaftLocation() != null ? s.getRaftLocation().getHost() : "",
                s.getRaftLocation() != null ? s.getRaftLocation().getPort() : 0,
                s.getStoreType() != null ? s.getStoreType().name() : "",
                s.getState() != null ? s.getState().name() : ""
            })
            .collect(Collectors.toList());
    }

    @Override
    public List<Object[]> getCoordinatorNodes() {
        try {
            return Services.parse(Configuration.coordinators()).stream()
                .map(l -> new Object[] {l.getHost(), l.getPort(), ""})
                .collect(Collectors.toList());
        } catch (Exception e) {
            LogUtils.error(log, "Get coordinator nodes failed: " + e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    @Override
    public int getRegionCount() {
        try {
            return coordinatorService.getRegionMap(
                TsoService.getDefault().cacheTso(),
                GetRegionMapRequest.builder().tenantId(TenantConstant.TENANT_ID).build()
            ).getRegionmap().getRegions().size();
        } catch (Exception e) {
            LogUtils.error(log, "Get region count failed: " + e.getMessage(), e);
            return 0;
        }
    }

    @Override
    public long getGcSafePoint() {
        try {
            GetGCSafePointRequest request = GetGCSafePointRequest.builder()
                .getAllTenant(false)
                .build();
            GetGCSafePointResponse response = coordinatorService.getGCSafePoint(
                TsoService.getDefault().cacheTso(), request
            );
            return response.getSafePoint();
        } catch (Exception e) {
            LogUtils.error(log, "Get GC safe point failed: " + e.getMessage(), e);
            return 0;
        }
    }

    @Override
    public List<Object[]> getJobList() {
        try {
            List<Job> jobs = JobManagerImpl.INSTANCE.jobList();
            return jobs.stream()
                .map(job -> {
                    String txnId = "";
                    String queryId = "";
                    if (job instanceof JobImpl) {
                        JobImpl jobImpl = (JobImpl) job;
                        txnId = jobImpl.getTxnId() != null ? jobImpl.getTxnId().toString() : "";
                        queryId = jobImpl.getQueryId() != null ? jobImpl.getQueryId() : "";
                    }
                    return new Object[] {
                        job.getJobId().toString(),
                        txnId,
                        job.getStartTime(),
                        job.isSelect(),
                        System.currentTimeMillis() - job.getStartTime(),
                        queryId,
                        job.dataCnt()
                    };
                })
                .collect(Collectors.toList());
        } catch (Exception e) {
            LogUtils.error(log, "Get job list failed: " + e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    private String url(io.dingodb.sdk.service.entity.common.Location location) {
        return location.getHost() + ":" + location.getPort();
    }

    public static void register() {
        Executors.scheduleWithFixedDelayAsync(
            "cluster-heartbeat",
            () -> executorHeartbeat(),
            0,
            10,
            TimeUnit.SECONDS
        );
    }

    private static void executorHeartbeat() {
        try {
            coordinatorService.executorHeartbeat(TsoService.getDefault().cacheTso(), executorHeartbeatRequest());
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        }
    }
}
