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

package io.dingodb.server.executor.ddl;

import io.dingodb.common.CommonId;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.ddl.ReorgBackFillTask;
import io.dingodb.common.ddl.ReorgInfo;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.server.executor.service.BackFiller;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
public final class BackFilling {
    public static final int typeAddIndexWorker = 0;
    public static final int typeAddColumnWorker = 1;

    private BackFilling() {
    }

    public static String writePhysicalTableRecord(int bfWorkerType, ReorgInfo reorgInfo) {
        DdlJob job = reorgInfo.getDdlJob();

        String error = Reorg.INSTANCE.isReorgRunnable(job.getId());
        if (error != null) {
            return error;
        }
        // fill table data to index
        CommonId tableId = reorgInfo.getTableId();
        Table table = DdlService.root().getTable(tableId);
        PartitionService ps = PartitionService.getService(
            Optional.ofNullable(table.getPartitionStrategy())
                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
        return backFill(bfWorkerType, reorgInfo, ps, job);
    }

    @Nullable
    private static String backFill(
        int bfWorkerType,
        ReorgInfo reorgInfo,
        PartitionService ps,
        DdlJob job
    ) {
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> regionMap
            = MetaService.root().getRangeDistribution(reorgInfo.getTableId());
        Set<RangeDistribution> distributions = ps.calcPartitionRange(null, null, true, true, regionMap);
        BackFiller filler;
        if (bfWorkerType == 0) {
            filler = new IndexAddFiller();
        } else if (bfWorkerType == 1) {
            filler = new AddColumnFiller();
        } else {
            throw new RuntimeException("do not support bf work type");
        }

        List<ReorgBackFillTask> taskList = distributions
            .stream()
            .map(region -> ReorgBackFillTask.builder()
                .tableId(reorgInfo.getTableId())
                .indexId(reorgInfo.getIndexId())
                .startTs(reorgInfo.getDdlJob().getSnapshotVer())
                .jobId(reorgInfo.getDdlJob().getId())
                .start(region.getStartKey())
                .end(region.getEndKey())
                .withStart(region.isWithStart())
                .withEnd(region.isWithEnd())
                .regionId(region.getId())
                .build()
            )
            .collect(Collectors.toList());
        List<ReorgBackFillTask> destTaskList = new ArrayList<>();
        boolean preWritePri = false;
        for (ReorgBackFillTask task : taskList) {
            if (!preWritePri) {
                try {
                    preWritePri = filler.preWritePrimary(task);
                } catch (RegionSplitException e) {
                    LogUtils.error(log, e.getMessage(), e);
                    return backFill(bfWorkerType, reorgInfo, ps, job);
                }
                if (preWritePri) {
                    destTaskList.add(task);
                }
            } else {
                destTaskList.add(task);
            }
        }
        LogUtils.info(log, "[ddl] pre write primary key done, bf type:{}, jobId:{}", bfWorkerType, job.getId());
        long start = System.currentTimeMillis();

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(destTaskList.stream().map(task -> {
            Callable<BackFillResult> callable = () -> fill(filler, task, false);
            return Executors.submit("reorg", callable);
        }).toArray(CompletableFuture[]::new));
        try {
            allFutures.get();
            long end = System.currentTimeMillis();
            LogUtils.info(log, "[ddl] pre second key done, " +
                "bf type:{}, jobId:{}, scanCount:{}, addCount:{}, conflict count:{}, cost:{}ms",
                bfWorkerType, job.getId(), filler.getScanCount(),
                filler.getAddCount(), filler.getConflictCount(), (end - start));
            start = System.currentTimeMillis();
            boolean commitPriRes = filler.commitPrimary();
            boolean commitSecondRes = filler.commitSecond();
            end = System.currentTimeMillis();
            long sub = (end - start);
            LogUtils.info(log, "[ddl] commit done, primary:{}, second:{}, bf type:{}, " +
                "jobId:{}, commitCnt:{}, cost:{}ms",
                commitPriRes, commitSecondRes, bfWorkerType, job.getId(), filler.getCommitCount(), sub);
        } catch (InterruptedException | ExecutionException e) {
            LogUtils.error(log, "pre write second error", e);
            if (e.getMessage().contains("RegionSplit")) {
                return backFillRegionSplit(bfWorkerType, reorgInfo, ps, job, filler);
            }
            return e.getMessage();
        } finally {
            filler.close();
        }
        return null;
    }

    public static BackFillResult fill(BackFiller filler, ReorgBackFillTask fillTask, boolean withCheck) {
        BackFillResult backFillResult = filler.backFillDataInTxn(fillTask, withCheck);
        ReorgCtx reorgCtx = DdlContext.INSTANCE.getReorgCtx1(fillTask.getJobId());
        reorgCtx.incrementCount(backFillResult.addCount);
        return backFillResult;
    }

    public static String backFillRegionSplit(
        int bfWorkerType,
        ReorgInfo reorgInfo,
        PartitionService ps,
        DdlJob job,
        BackFiller filler
    ) {
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> regionMap
            = MetaService.root().getRangeDistribution(reorgInfo.getTableId());
        Set<RangeDistribution> distributions = ps.calcPartitionRange(null, null, true, true, regionMap);

        List<ReorgBackFillTask> taskList = distributions
            .stream()
            .filter(distribution -> !filler.getDoneRegion().contains(distribution.getId()))
            .map(region -> ReorgBackFillTask.builder()
                .tableId(reorgInfo.getTableId())
                .indexId(reorgInfo.getIndexId())
                .startTs(reorgInfo.getDdlJob().getSnapshotVer())
                .jobId(reorgInfo.getDdlJob().getId())
                .start(region.getStartKey())
                .end(region.getEndKey())
                .withStart(region.isWithStart())
                .withEnd(region.isWithEnd())
                .regionId(region.getId())
                .build()
            )
            .collect(Collectors.toList());

        long start = System.currentTimeMillis();

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(taskList.stream().map(task -> {
            Callable<BackFillResult> callable = () -> fillWithCheck(filler, task, true);
            return Executors.submit("reorg", callable);
        }).toArray(CompletableFuture[]::new));
        try {
            allFutures.get();
            long end = System.currentTimeMillis();
            LogUtils.info(log, "[ddl] region split pre second key done, " +
                    "bf type:{}, jobId:{}, scanCount:{}, addCount:{}, conflict count:{}, cost:{}ms",
                bfWorkerType, job.getId(), filler.getScanCount(),
                filler.getAddCount(), filler.getConflictCount(), (end - start));
            start = System.currentTimeMillis();
            boolean commitPriRes = filler.commitPrimary();
            boolean commitSecondRes = filler.commitSecond();
            end = System.currentTimeMillis();
            long sub = (end - start);
            LogUtils.info(log, "[ddl] region split commit done, primary:{}, second:{}, bf type:{}, " +
                    "jobId:{}, commitCnt:{}, cost:{}ms",
                commitPriRes, commitSecondRes, bfWorkerType, job.getId(), filler.getCommitCount(), sub);
        } catch (InterruptedException | ExecutionException e) {
            LogUtils.error(log, "region split pre write second error", e);
            if (e.getMessage().contains("RegionSplit")) {
                return backFillRegionSplit(bfWorkerType, reorgInfo, ps, job, filler);
            }
            return e.getMessage();
        }
        return null;
    }

    public static BackFillResult fillWithCheck(BackFiller filler, ReorgBackFillTask fillTask, boolean withCheck) {
        BackFillResult backFillResult = filler.backFillDataInTxnWithCheck(fillTask, withCheck);
        ReorgCtx reorgCtx = DdlContext.INSTANCE.getReorgCtx1(fillTask.getJobId());
        reorgCtx.incrementCount(backFillResult.addCount);
        return backFillResult;
    }

}
