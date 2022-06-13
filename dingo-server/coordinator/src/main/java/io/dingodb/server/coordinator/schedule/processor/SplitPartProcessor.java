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

package io.dingodb.server.coordinator.schedule.processor;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.util.Optional;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.meta.adaptor.impl.ExecutorAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.ReplicaAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.SplitTaskAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TableAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TablePartAdaptor;
import io.dingodb.server.coordinator.schedule.SplitTask;
import io.dingodb.server.coordinator.schedule.TableScheduler;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.server.protocol.meta.Replica;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePart;
import io.dingodb.server.protocol.meta.TablePartStats;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.dingodb.server.coordinator.schedule.processor.TableStoreProcessor.applyTablePart;

@Getter
@Setter
@Accessors(fluent = true)
@Slf4j
public class SplitPartProcessor {

    private final TableAdaptor tableAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Table.class);
    private final TablePartAdaptor tablePartAdaptor = MetaAdaptorRegistry.getMetaAdaptor(TablePart.class);
    private final SplitTaskAdaptor splitTaskAdaptor = MetaAdaptorRegistry.getMetaAdaptor(SplitTask.class);
    private final ReplicaAdaptor replicaAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Replica.class);
    private final ExecutorAdaptor executorAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Executor.class);

    private final TableScheduler tableScheduler;
    private final CommonId tableId;

    public SplitPartProcessor(TableScheduler tableScheduler, CommonId tableId) {
        this.tableScheduler = tableScheduler;
        this.tableId = tableId;
    }

    public CompletableFuture<Void> splitPart(TablePartStats stats) {
        try {
            byte[] splitKey = estimateSplitKey(stats);
            if (splitKey == null) {
                return CompletableFuture.completedFuture(null);
            }
            if (Arrays.equals(tablePartAdaptor.get(stats.getTablePart()).getEnd(), splitKey)) {
                return CompletableFuture.completedFuture(null);
            }
            return split(stats.getTablePart(), splitKey);
        } catch (Exception e) {
            log.error("Split table part error, part: {}", stats.getTablePart(), e);
            return CompletableFuture.completedFuture(null);
        }
    }

    public CompletableFuture<Void> split(CommonId part, byte[] split) {
        SplitTask task = createTask(part, split);
        return CompletableFuture.runAsync(() -> processSplitTask(task));
    }

    public byte[] estimateSplitKey(TablePartStats stats) {
        List<TablePartStats.ApproximateStats> approximateStats = stats.getApproximateStats();
        Table table = tableAdaptor.get(tableId);
        if (!table.isAutoSplit() || approximateStats == null || approximateStats.size() <= 1) {
            return null;
        }
        long totalCount = 0;
        long totalSize = 0;
        byte[] halfCountKey = null;
        byte[] halfSizeKey = null;
        byte[] splitKey = null;
        long halfSize = table.getPartMaxSize() / 2;
        long halfCount = table.getPartMaxCount() / 2;
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
            if (totalSize >= table.getPartMaxSize()) {
                splitKey = halfSizeKey;
                break;
            }
            if (totalCount >= table.getPartMaxCount()) {
                splitKey = halfCountKey;
                break;
            }
        }
        return splitKey;
    }

    public void processSplitTask(SplitTask task) {
        TablePart oldPart = tablePartAdaptor.get(task.getOldPart());
        if (Arrays.equals(oldPart.getEnd(), task.getSplitKey())) {
            task.setStep(SplitTask.Step.IGNORE);
            return;
        }
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

    private void reassignPart(TablePart oldPart, SplitTask task) {
        List<Replica> replicas = replicaAdaptor.getByDomain(oldPart.getId().seqContent());
        List<Location> locations = replicas.stream().map(Replica::location).collect(Collectors.toList());
        replicas.forEach(replica -> applyTablePart(oldPart, replica.getExecutor(), locations, true));
        task.setStep(SplitTask.Step.FINISH);
        splitTaskAdaptor.save(task);
    }

    public void updateOldPart(SplitTask task, TablePart oldPart) {
        tableAdaptor.updatePart(task.getOldPart(), oldPart.getStart(), task.getSplitKey());
        task.setStep(SplitTask.Step.REASSIGN_PART);
        splitTaskAdaptor.save(task);
    }

    public TablePart createNewPart(byte[] start, SplitTask task, TablePart oldPart) {
        TablePart newPart = tableAdaptor.newPart(oldPart.getTable(), start, oldPart.getEnd());
        task.setStep(SplitTask.Step.START_NEW_PART);
        task.setNewPart(newPart.getId());
        splitTaskAdaptor.save(task);
        return newPart;
    }

    public void startNewPart(SplitTask task, TablePart newPart) {
        tableScheduler.assignPart(newPart).join();
        task.setStep(SplitTask.Step.UPDATE_OLD_PART);
        splitTaskAdaptor.save(task);
    }

    public SplitTask createTask(CommonId part, byte[] splitKey) {
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
