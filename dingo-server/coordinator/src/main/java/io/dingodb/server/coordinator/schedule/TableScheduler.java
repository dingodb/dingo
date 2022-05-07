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
import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.dingodb.common.util.Optional;
import io.dingodb.net.NetAddress;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.TableStoreApi;
import io.dingodb.server.coordinator.config.CoordinatorConfiguration;
import io.dingodb.server.coordinator.config.ScheduleConfiguration;
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
import io.dingodb.store.api.Part;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.dingodb.common.error.CommonError.EXEC;
import static io.dingodb.common.error.CommonError.EXEC_INTERRUPT;
import static io.dingodb.common.error.CommonError.EXEC_TIMEOUT;

@Slf4j
public class TableScheduler {

    public static final NetService NET_SERVICE = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private final Table table;

    private final ExecutorService executorService = new ThreadPoolBuilder()
        .maximumThreads(Integer.MAX_VALUE)
        .keepAliveSeconds(60L)
        .name("ClusterScheduler")
        .build();

    private TableAdaptor tableAdaptor;
    private TablePartAdaptor tablePartAdaptor;
    private ReplicaAdaptor replicaAdaptor;
    private ExecutorAdaptor executorAdaptor;
    private SplitTaskAdaptor splitTaskAdaptor;

    private TablePartStatsAdaptor tablePartStatsAdaptor;
    private ExecutorStatsAdaptor executorStatsAdaptor;

    private ScheduleConfiguration scheduleConfiguration = CoordinatorConfiguration.schedule();
    private long partMaxSize = scheduleConfiguration.getDefaultAutoMaxSize();
    private long partMaxCount = scheduleConfiguration.getDefaultAutoMaxCount();

    private Map<CommonId, TableStoreApi> tableStoreApis = new ConcurrentHashMap<>();
    private Map<CommonId, CompletableFuture<Void>> waitFutures = new ConcurrentHashMap<>();

    private final AtomicBoolean busy = new AtomicBoolean(false);

    public TableScheduler(Table table) {
        this.table = table;

        if (table.getPartMaxCount() > 0) {
            partMaxCount = table.getPartMaxCount();
        }
        if (table.getPartMaxSize() > 0) {
            partMaxSize = table.getPartMaxSize();
        }

        this.tableAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Table.class);
        this.tablePartAdaptor = MetaAdaptorRegistry.getMetaAdaptor(TablePart.class);
        this.replicaAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Replica.class);
        this.executorAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Executor.class);
        this.splitTaskAdaptor = MetaAdaptorRegistry.getMetaAdaptor(SplitTask.class);

        this.tablePartStatsAdaptor = MetaAdaptorRegistry.getStatsMetaAdaptor(TablePartStats.class);
        this.executorStatsAdaptor = MetaAdaptorRegistry.getStatsMetaAdaptor(ExecutorStats.class);

        ApiRegistry apiRegistry = NET_SERVICE.apiRegistry();
        executorAdaptor.getAll().stream().forEach(executor -> tableStoreApis.put(
            executor.getId(),
            apiRegistry.proxy(TableStoreApi.class, () -> new NetAddress(executor.getHost(), executor.getPort())))
        );
        splitTaskAdaptor.getByDomain(PrimitiveCodec.encodeInt(1))
            .forEach((task -> executorService.submit(() -> {
                try {
                    busy.compareAndSet(false, true);
                    processSplitTask(task);
                } finally {
                    busy.set(false);
                }
            })));
    }

    public void addStore(CommonId id, Location location) {
        tableStoreApis.put(
            id,
            NET_SERVICE.apiRegistry().proxy(
                TableStoreApi.class,
                () -> new NetAddress(location.getHost(), location.getPort())
            )
        );
    }

    public void deleteTable() {
        for (Map.Entry<CommonId, TableStoreApi> entry : tableStoreApis.entrySet()) {
            log.info("Delete table on {}", entry.getKey());
            entry.getValue().deleteTable(table.getId());
        }
    }

    public void assignPart(TablePart tablePart) {
        log.info("On assign table part, part: {}", tablePart);
        List<Replica> replicas = createReplicas(tablePart);
        List<Location> locations = replicas.stream().map(Replica::location).collect(Collectors.toList());
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
            log.info("Start part replica on executor cnt: {}, current: {}\n part: {}", replicas.size(), replica, part);
            TableStoreApi storeApi = tableStoreApis.get(replica.getExecutor());
            storeApi.assignTablePart(part);
        }
        try {
            future.get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            EXEC_INTERRUPT.throwFormatError("wait stats report", Thread.currentThread(), tablePart.getId());
        } catch (ExecutionException e) {
            EXEC.throwFormatError("wait stats report", Thread.currentThread(), tablePart.getId());
        } catch (TimeoutException e) {
            EXEC_TIMEOUT.throwFormatError("wait stats report", Thread.currentThread(), tablePart.getId());
        }
    }

    private void reassignPart(TablePart oldPart, SplitTask task) {
        List<Replica> replicas = replicaAdaptor.getByDomain(oldPart.getId().seqContent());

        Part part = Part.builder()
            .id(oldPart.getId())
            .instanceId(oldPart.getTable())
            .type(Part.PartType.ROW_STORE)
            .start(oldPart.getStart())
            .end(oldPart.getEnd())
            .replicates(replicas.stream().map(Replica::location).collect(Collectors.toList()))
            .build();

        for (Replica replica : replicas) {
            log.info("Reassign part replica on executor, current: {}", replica);
            TableStoreApi storeApi = tableStoreApis.get(replica.getExecutor());
            storeApi.reassignTablePart(part);
        }
        task.setStep(SplitTask.Step.FINISH);
        splitTaskAdaptor.save(task);
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

    public boolean processStats(TablePartStats stats) {
        if (log.isDebugEnabled()) {
            log.debug("Receive stats: {}", stats);
        }
        executorService.submit(() -> {
            tablePartStatsAdaptor.onStats(stats);
            Optional.ofNullable(waitFutures.remove(stats.getTablePart())).ifPresent(future -> future.complete(null));
            splitPart(stats);
        });
        return true;
    }

    public byte[] estimateSplitKey(TablePartStats stats) {
        List<TablePartStats.ApproximateStats> approximateStats = stats.getApproximateStats();
        if (!scheduleConfiguration.isAutoSplit() || approximateStats == null || approximateStats.size() <= 1) {
            return null;
        }
        long totalCount = 0;
        long totalSize = 0;
        byte[] halfCountKey = null;
        byte[] halfSizeKey = null;
        byte[] splitKey = null;
        long halfSize = partMaxSize / 2;
        long halfCount = partMaxCount / 2;
        for (TablePartStats.ApproximateStats approximateStat : approximateStats) {
            totalCount += approximateStat.getCount();
            totalSize += approximateStat.getSize();
            if (totalSize >= halfSize && halfSizeKey == null) {
                if (totalSize - halfSize > halfSize - totalSize + approximateStat.getSize()) {
                    halfSizeKey = approximateStat.getStartKey();
                } else {
                    halfSizeKey = approximateStat.getEndKey();
                }
            }
            if (totalCount >= halfCount && halfCountKey == null) {
                if (totalCount - halfCount > halfCount - totalCount + approximateStat.getCount()) {
                    halfCountKey = approximateStat.getStartKey();
                } else {
                    halfCountKey = approximateStat.getEndKey();
                }
            }
            if (totalSize >= partMaxSize) {
                splitKey = halfSizeKey;
                break;
            }
            if (totalCount >= partMaxCount) {
                splitKey = halfCountKey;
                break;
            }
        }
        return splitKey;
    }

    private void splitPart(TablePartStats stats) {
        if (!busy.compareAndSet(false, true)) {
            return;
        }
        try {
            byte[] splitKey = estimateSplitKey(stats);
            if (splitKey == null) {
                return;
            }
            SplitTask task = createTask(stats.getTablePart(), splitKey);
            processSplitTask(task);
        } catch (Exception e) {
            log.error("Split table part error, part: {}", stats.getTablePart(), e);
        } finally {
            busy.set(false);
        }
    }

    private void processSplitTask(SplitTask task) {
        TablePart oldPart = tablePartAdaptor.get(task.getOldPart());
        TablePart newPart = tablePartAdaptor.get(task.getNewPart());
        if (task.getStep() == SplitTask.Step.CREATE_NEW_PART) {
            newPart = createNewPart(task.getSplitKey(), task, oldPart);
        }
        if (task.getStep() == SplitTask.Step.START_NEW_PART) {
            startNewPart(task, newPart);
        }
        if (task.getStep() == SplitTask.Step.UPDATE_OLD_PART) {
            updateOldPart(task, oldPart);
        }
        if (task.getStep() == SplitTask.Step.REASSIGN_PART) {
            reassignPart(oldPart, task);
        }
    }

    private void updateOldPart(SplitTask task, TablePart oldPart) {
        tableAdaptor.updatePart(task.getOldPart(), oldPart.getStart(), task.getSplitKey());
        task.setStep(SplitTask.Step.REASSIGN_PART);
        splitTaskAdaptor.save(task);
    }

    private TablePart createNewPart(byte[] start, SplitTask task, TablePart oldPart) {
        TablePart newPart = tableAdaptor.newPart(oldPart.getTable(), start, oldPart.getEnd());
        task.setStep(SplitTask.Step.START_NEW_PART);
        task.setNewPart(newPart.getId());
        splitTaskAdaptor.save(task);
        return newPart;
    }

    private void startNewPart(SplitTask task, TablePart newPart) {
        assignPart(newPart);
        task.setStep(SplitTask.Step.UPDATE_OLD_PART);
        splitTaskAdaptor.save(task);
    }

    private SplitTask createTask(CommonId part, byte[] splitKey) {
        SplitTask task = Optional.of(splitTaskAdaptor.getByDomain(part.seqContent()))
            .filter(tasks -> !tasks.isEmpty())
            .map(tasks -> tasks.get(0))
            .ifAbsentSet(() -> splitTaskAdaptor.newTask(part))
            .orNull();
        task.setStep(SplitTask.Step.CREATE_NEW_PART);
        task.setSplitKey(splitKey);
        splitTaskAdaptor.save(task);
        return task;
    }

}
