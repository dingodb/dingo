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
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.store.Part;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.api.ReportApi;
import io.dingodb.server.api.ServerApi;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.meta.adaptor.impl.ExecutorAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.ExecutorStatsAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.ReplicaAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.SplitTaskAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TableAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TablePartAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TablePartStatsAdaptor;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.server.protocol.meta.ExecutorStats;
import io.dingodb.server.protocol.meta.Replica;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePart;
import io.dingodb.server.protocol.meta.TablePartStats;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class ClusterScheduler implements ServerApi, ReportApi {

    private static final ClusterScheduler INSTANCE = new ClusterScheduler();

    public static ClusterScheduler instance() {
        return INSTANCE;
    }

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private TableAdaptor tableAdaptor;
    private TablePartAdaptor tablePartAdaptor;
    private ReplicaAdaptor replicaAdaptor;
    private ExecutorAdaptor executorAdaptor;
    private SplitTaskAdaptor splitTaskAdaptor;

    private TablePartStatsAdaptor tablePartStatsAdaptor;
    private ExecutorStatsAdaptor executorStatsAdaptor;

    private Map<CommonId, TableScheduler> tableSchedulers;

    private ClusterScheduler() {
    }

    public void init() {

        tableAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Table.class);
        tablePartAdaptor = MetaAdaptorRegistry.getMetaAdaptor(TablePart.class);
        replicaAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Replica.class);
        executorAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Executor.class);
        splitTaskAdaptor = MetaAdaptorRegistry.getMetaAdaptor(SplitTask.class);

        tablePartStatsAdaptor = MetaAdaptorRegistry.getStatsMetaAdaptor(TablePartStats.class);
        executorStatsAdaptor = MetaAdaptorRegistry.getStatsMetaAdaptor(ExecutorStats.class);

        tableSchedulers = new ConcurrentHashMap<>();

        netService.apiRegistry().register(ServerApi.class, this);
        netService.apiRegistry().register(ReportApi.class, this);

        tableAdaptor.getAll().forEach(table -> tableSchedulers.put(table.getId(), new TableScheduler(table)));
    }

    public TableScheduler getTableScheduler(CommonId id) {
        return tableSchedulers.computeIfAbsent(id, __ -> new TableScheduler(tableAdaptor.get(__)));
    }

    public void deleteTableScheduler(CommonId id) {
        tableSchedulers.remove(id);
    }

    @Override
    public CommonId registerExecutor(Executor executor) {
        log.info("Register executor {}", executor);
        CommonId id = executorAdaptor.save(executor);
        Executors.submit("add-executor-to-tables", () ->
            tableSchedulers.values().forEach(scheduler -> scheduler.addStore(executor.getId(), executor.location()))
        );
        log.info("Register executor success id: [{}], {}.", id, executor);
        return id;
    }

    @Override
    public List<Part> storeMap(CommonId id) {
        List<Replica> replicas = replicaAdaptor.getByExecutor(id);
        if (replicas == null) {
            return Collections.emptyList();
        }
        log.info("Executor get store map, id: [{}], replicas: [{}] ==> {}", id, replicas.size(), replicas);
        return replicas.stream()
            .map(Replica::getPart)
            .map(tablePartAdaptor::get)
            .filter(Objects::nonNull)
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

    @Override
    public boolean report(TablePartStats stats) {
        return tableSchedulers.get(stats.getTable()).processStats(stats);
    }

    @Override
    public boolean report(ExecutorStats stats) {
        return true;
    }

}
