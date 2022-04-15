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

package io.dingodb.server.coordinator.schedule;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.dingodb.net.NetAddress;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.ReportApi;
import io.dingodb.server.api.ServerApi;
import io.dingodb.server.api.TableStoreApi;
import io.dingodb.server.coordinator.config.CoordinatorConfiguration;
import io.dingodb.server.coordinator.config.ScheduleConfiguration;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.meta.adaptor.impl.ExecutorAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.ExecutorStatsAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.ReplicaAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TableAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TablePartAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TablePartStatsAdaptor;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.server.protocol.meta.ExecutorStats;
import io.dingodb.server.protocol.meta.Replica;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePart;
import io.dingodb.server.protocol.meta.TablePartStats;
import io.dingodb.server.protocol.meta.TablePartStats.ApproximateStats;
import io.dingodb.store.api.Part;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class Scheduler implements ServerApi, ReportApi {

    private static final Scheduler INSTANCE = new Scheduler();

    public static Scheduler instance() {
        return INSTANCE;
    }

    private final ExecutorService executorService = new ThreadPoolBuilder()
        .maximumThreads(Integer.MAX_VALUE)
        .keepAliveSeconds(60L)
        .name("CoordinatorScheduler")
        .build();

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private Map<CommonId, TableStoreApi> tableStoreApis;

    private TableAdaptor tableAdaptor;
    private TablePartAdaptor tablePartAdaptor;
    private ReplicaAdaptor replicaAdaptor;
    private ExecutorAdaptor executorAdaptor;

    private TablePartStatsAdaptor tablePartStatsAdaptor;
    private ExecutorStatsAdaptor executorStatsAdaptor;

    private Map<CommonId, CompletableFuture<Void>> waitFutures;

    private ScheduleConfiguration scheduleConfiguration = CoordinatorConfiguration.instance().getSchedule();

    private Scheduler() {
    }

    public void init() {

        tableAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Table.class);
        tablePartAdaptor = MetaAdaptorRegistry.getMetaAdaptor(TablePart.class);
        replicaAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Replica.class);
        executorAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Executor.class);

        tablePartStatsAdaptor = MetaAdaptorRegistry.getStatsMetaAdaptor(TablePartStats.class);
        executorStatsAdaptor = MetaAdaptorRegistry.getStatsMetaAdaptor(ExecutorStats.class);

        tableStoreApis = new ConcurrentHashMap<>();
        ApiRegistry apiRegistry = netService.apiRegistry();
        executorAdaptor.getAll().stream().forEach(executor -> tableStoreApis.put(
            executor.getId(),
            apiRegistry.proxy(TableStoreApi.class, () -> new NetAddress(executor.getHost(), executor.getPort())))
        );

        netService.apiRegistry().register(ServerApi.class, this);
        netService.apiRegistry().register(ReportApi.class, this);

        waitFutures = new ConcurrentHashMap<>();
    }

    public void onDeleteTable(CommonId tableId, List<TablePart> tableParts) {
        log.info("On delete table: [{}], table parts: [{}] ==> {}", tableId, tableParts.size(), tableParts);
        tableParts.stream()
            .flatMap(tablePart -> replicaAdaptor.getByDomain(tablePart.getId().seqContent()).stream())
            .parallel()
            .peek(replica -> replicaAdaptor.delete(replica.getId()))
            .peek(replica -> log.info("Delete replica: {}", replica))
            .map(Replica::getExecutor)
            .distinct()
            .map(tableStoreApis::get)
            .forEach(api -> api.deleteTable(tableId));
    }

    public void onCreateTable(TablePart tablePart) throws Exception {
        log.info("On create table, default part: {}", tablePart);
        List<Replica> replicas = createReplicas(tablePart);
        List<Location> locations = replicas.stream()
            .map(replica -> new Location(replica.getHost(), replica.getPort()))
            .collect(Collectors.toList());
        Part part = Part.builder()
            .id(tablePart.getId())
            .instanceId(tablePart.getTable())
            .type(Part.PartType.ROW_STORE)
            .start(tablePart.getStart())
            .end(tablePart.getEnd())
            .replicates(locations)
            .build();
        CompletableFuture<Void> future = new CompletableFuture<>();
        waitFutures.put(tablePart.getId(), future);
        for (Replica replica : replicas) {
            log.info("Start default part replica on executor replicas: [{}], current: {}", replicas.size(), replica);
            TableStoreApi storeApi = tableStoreApis.get(replica.getExecutor());
            storeApi.newTablePart(part);
        }
        future.get(30, TimeUnit.SECONDS);
    }

    protected List<Replica> createReplicas(TablePart tablePart) {
        log.info("Create replicas for table part: {}", tablePart);
        if (tablePart.getReplicas() == 0) {
            return executorAdaptor.getAll().stream()
                .map(executor -> Replica.builder()
                    .host(executor.getHost())
                    .part(tablePart.getId())
                    .port(executor.getPort())
                    .executor(executor.getId())
                    .build()
                ).peek(replicaAdaptor::save)
                .collect(Collectors.toList());
        }
        return null;
    }

    @Override
    public CommonId registerExecutor(Executor executor) {
        log.info("Register executor {}", executor);
        CommonId id = executorAdaptor.save(executor);
        tableStoreApis.put(
            id,
            netService.apiRegistry().proxy(
                TableStoreApi.class,
                () -> new NetAddress(executor.getHost(), executor.getPort())
            )
        );
        log.info("Register executor success id: [{}], {}.", id, executor);
        return id;
    }

    @Override
    public List<Part> storeMap(CommonId id) {
        List<Replica> replicas = replicaAdaptor.getByExecutor(id);
        log.info("Executor get store map, id: [{}], replicas: [{}] ==> {}", id, replicas.size(), replicas);
        return replicas.stream()
            .map(Replica::getPart)
            .map(tablePartAdaptor::get)
            .map(tablePart -> Part.builder()
                .id(tablePart.getId())
                .instanceId(tablePart.getTable())
                .start(tablePart.getStart())
                .end(tablePart.getEnd())
                .type(Part.PartType.ROW_STORE)
                .replicates(replicaAdaptor.getByDomain(tablePart.getId().seqContent()).stream()
                    .map(Replica::location)
                    .collect(Collectors.toList()))
                .build())
            .collect(Collectors.toList());
    }

    private void onTablePartStats(TablePartStats stats) {
        List<ApproximateStats> approximateStats = stats.getApproximateStats();
        long totalCount = 0;
        long totalSize = 0;
        byte[] halfCountKey = null;
        byte[] halfSizeKey = null;
        byte[] splitKey = null;
        for (ApproximateStats approximateStat : approximateStats) {
            totalCount += approximateStat.getCount();
            totalSize += approximateStat.getSize();
            if (totalSize >= scheduleConfiguration.getDefaultAutoMaxSize() / 2) {
                halfSizeKey = approximateStat.getStartKey();
            }
            if (totalCount >= scheduleConfiguration.getDefaultAutoMaxCount() / 2) {
                halfCountKey = approximateStat.getStartKey();
            }
            if (totalSize >= scheduleConfiguration.getDefaultAutoMaxSize()) {
                splitKey = halfSizeKey;
            }
            if (totalCount >= scheduleConfiguration.getDefaultAutoMaxSize()) {
                splitKey = halfCountKey;
            }
        }
        if (splitKey == null) {
            return;
        }
        if (scheduleConfiguration.isAutoSplit()) {
            // skip
        }
    }

    @Override
    public boolean report(TablePartStats stats) {
        if (log.isDebugEnabled()) {
            log.debug("Receive stats: {}", stats);
        }
        tablePartStatsAdaptor.onStats(stats);
        Optional.ofNullable(waitFutures.remove(stats.getTablePart())).ifPresent(future -> future.complete(null));
        return true;
    }

    @Override
    public boolean report(ExecutorStats stats) {
        return true;
    }
}
