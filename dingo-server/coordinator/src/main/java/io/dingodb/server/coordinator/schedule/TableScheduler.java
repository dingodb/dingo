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
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.util.Optional;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.meta.adaptor.impl.ExecutorAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.ReplicaAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.SplitTaskAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TableAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TablePartAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TablePartStatsAdaptor;
import io.dingodb.server.coordinator.schedule.processor.SplitPartProcessor;
import io.dingodb.server.coordinator.schedule.processor.TableStoreProcessor;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.server.protocol.meta.Replica;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePart;
import io.dingodb.server.protocol.meta.TablePartStats;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry.getStatsMetaAdaptor;
import static io.dingodb.server.coordinator.schedule.processor.TableStoreProcessor.applyTablePart;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.STATS_IDENTIFIER;

@Slf4j
public class TableScheduler {

    public static final NetService NET_SERVICE = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private final CommonId tableId;
    private final SplitPartProcessor splitPartProcessor;

    private TableAdaptor tableAdaptor;
    private TablePartAdaptor tablePartAdaptor;
    private ReplicaAdaptor replicaAdaptor;
    private ExecutorAdaptor executorAdaptor;
    private SplitTaskAdaptor splitTaskAdaptor;

    private TablePartStatsAdaptor tablePartStatsAdaptor;

    private Map<CommonId, CompletableFuture<Void>> reportFutures = new ConcurrentHashMap<>();

    private final AtomicBoolean busy = new AtomicBoolean(false);

    public TableScheduler(Table table) {
        this.tableId = table.getId();
        this.splitPartProcessor = new SplitPartProcessor(this, tableId);
        this.tableAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Table.class);
        this.tablePartAdaptor = MetaAdaptorRegistry.getMetaAdaptor(TablePart.class);
        this.replicaAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Replica.class);
        this.executorAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Executor.class);
        this.splitTaskAdaptor = MetaAdaptorRegistry.getMetaAdaptor(SplitTask.class);

        this.tablePartStatsAdaptor = getStatsMetaAdaptor(TablePartStats.class);

        splitTaskAdaptor.getByDomain(PrimitiveCodec.encodeInt(1))
            .forEach((task -> Executors.submit("split-" + task.getId().toString(), () -> {
                try {
                    busy.compareAndSet(false, true);
                    if (task.getStep() == SplitTask.Step.IGNORE) {
                        return;
                    }
                    splitPartProcessor.processSplitTask(task);
                } finally {
                    busy.set(false);
                }
            })));
    }

    public void addReplica(CommonId partId, CommonId executorId) {
        if (!busy.compareAndSet(false, true)) {
            throw new RuntimeException("Busy.");
        }
        try {
            log.info("Add part [{}] replica on [{}]", partId, executorId);
            TablePart tablePart = tablePartAdaptor.get(partId);
            Replica replica = replicaAdaptor.getByExecutor(executorId, partId);
            if (replica == null) {
                replica = replicaAdaptor.newReplica(tablePart, executorAdaptor.get(executorId));
                log.info("Save mate for part [{}] replica [{}] on [{}]", partId, replica.getId(), executorId);
                replicaAdaptor.save(replica);
            } else {
                log.info("The replica meta [{}] on [{}] is exist.", partId, executorId);
            }
            TablePartStats partStats = tablePartStatsAdaptor.getStats(
                new CommonId(ID_TYPE.stats, STATS_IDENTIFIER.part, partId.domain(), partId.seqContent()));
            List<Location> locations = replicaAdaptor.getLocationsByDomain(partId.seqContent());
            log.info("Update part [{}] on leader.", partId);
            try {
                applyTablePart(tablePart, partStats.getLeader(), locations, true);
            } catch (Exception e) {
                log.error("Update part [{}] on leader error.", partId, e);
            }
            try {
                log.info("Start part [{}] replica [{}] on [{}].", partId, replica.getId(), executorId);
                applyTablePart(tablePart, executorId, locations, false);
            } catch (Exception e) {
                log.info("Start part [{}] replica [{}] on [{}] error.", partId, replica.getId(), executorId, e);
            }
            log.info("Add part [{}] replica [{}] on [{}] finish", partId, replica.getId(), executorId);
        } catch (Exception e ) {
            System.out.println(e);
        }
        finally {
            busy.set(false);
        }
    }

    public void removeReplica(CommonId partId, CommonId executorId) {
        if (!busy.compareAndSet(false, true)) {
            throw new RuntimeException("Busy.");
        }
        try {
            log.info("Remove part [{}] replica on [{}]", partId, executorId);
            TablePart tablePart = tablePartAdaptor.get(partId);
            Replica replica = replicaAdaptor.getByExecutor(executorId, partId);
            if (replica == null) {
                log.info("Not found replica meta [{}] on [{}].", partId, executorId);
            } else {
                log.info("Delete mata for part [{}] replica [{}] on [{}]", partId, replica.getId(), executorId);
                replicaAdaptor.delete(replica.getId());
            }
            TablePartStats partStats = tablePartStatsAdaptor.getStats(
                new CommonId(ID_TYPE.stats, STATS_IDENTIFIER.part, partId.domain(), partId.seqContent()));
            List<Location> locations = replicaAdaptor.getLocationsByDomain(partId.seqContent());
            log.info("Update part [{}] on leader.", partId);
            try {
                applyTablePart(tablePart, partStats.getLeader(), locations, true);
            } catch (Exception e) {
                log.error("Update part [{}] on leader.", partId, e);
            }
            log.info("Stop part [{}] replica on [{}].", partId, executorId);
            try {
                TableStoreProcessor.removeReplica(executorId, tablePart);
            } catch (Exception e) {
                log.warn("Stop part [{}] replica on [{}] error.", partId, executorId, e);
            }
            log.info("Remove part replica [{}] on [{}] finish", partId, executorId);
        } finally {
            busy.set(false);
        }
    }

    public void transferLeader(CommonId partId, CommonId executorId) {
        if (!busy.compareAndSet(false, true)) {
            throw new RuntimeException("Busy.");
        }
        try {
            log.info("Transfer leader part [{}] replica to [{}]", partId, executorId);
            TablePart tablePart = tablePartAdaptor.get(partId);
            Replica replica = replicaAdaptor.getByExecutor(executorId, partId);
            if (replica == null) {
                throw new RuntimeException("Not found part on executor.");
            }
            TablePartStats partStats = tablePartStatsAdaptor.getStats(
                new CommonId(ID_TYPE.stats, STATS_IDENTIFIER.part, partId.domain(), partId.seqContent()));
            List<Location> locations = replicaAdaptor.getLocationsByDomain(partId.seqContent());
            log.info("Update part [{}] on current leader.", partId);
            try {
                applyTablePart(tablePart, partStats.getLeader(), locations, replica.location(), true);
            } catch (Exception e) {
                log.error("Update part [{}] on current leader error.", partId, e);
            }
            log.info("Transfer leader replica [{}] to [{}] finish", partId, executorId);
        } finally {
            busy.set(false);
        }
    }

    public void split(CommonId part) {
        TablePartStats stats = getStatsMetaAdaptor(TablePartStats.class).getStats(
            new CommonId(ID_TYPE.stats, STATS_IDENTIFIER.part, part.domain(), part.seqContent())
        );
        if (stats.getApproximateStats().size() < 2) {
            log.warn("The part approximate stats size less than 2, unsupported split.");
            return;
        }
        int index = stats.getApproximateStats().size() / 2;
        split(part, stats.getApproximateStats().get(index).getEndKey());
    }

    public void split(CommonId part, byte[] split) {
        runTask(() -> splitPartProcessor.split(part, split));
    }

    public void deleteTable() {
        log.info("Delete table {}", tableId);
        TableStoreProcessor.deleteTable(tableId);
    }

    public CompletableFuture<Void> assignPart(TablePart tablePart) {
        log.info("On assign table part, part: {}", tablePart);
        CompletableFuture<Void> future = new CompletableFuture<>();
        List<Replica> replicas = replicaAdaptor.getByDomain(tablePart.getId().seqContent());
        if (replicas == null || replicas.isEmpty()) {
            replicas = replicaAdaptor.createByPart(tablePart, executorAdaptor.getAll());
        }
        List<Location> locations = replicas.stream().map(Replica::location).collect(Collectors.toList());
        reportFutures.put(tablePart.getId(), future);
        replicas.forEach(replica -> applyTablePart(tablePart, replica.getExecutor(), locations, false));
        return future;
    }

    public boolean processStats(TablePartStats stats) {
        if (log.isTraceEnabled()) {
            log.trace("Receive stats: {}", stats);
        }
        Executors.submit("on-stats-" + stats.getId().toString(), () -> {
            tablePartStatsAdaptor.onStats(stats);
            Optional.ofNullable(reportFutures.remove(stats.getTablePart())).ifPresent(future -> future.complete(null));
            runTask(() -> splitPartProcessor.splitPart(stats));
        });
        return true;
    }

    public void runTask(Supplier<CompletableFuture<Void>> task) {
        if (busy.compareAndSet(false, true)) {
            task.get().whenComplete((r, e) -> {
                busy.set(false);
            });
        }
    }
}
