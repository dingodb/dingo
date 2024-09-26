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

package io.dingodb.exec.operator;

import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.RangeUtils;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.DistributionSourceParam;
import io.dingodb.meta.MetaService;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.transaction.exception.LockWaitException;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class NewCalcDistributionOperator extends SourceOperator {
    public static final NewCalcDistributionOperator INSTANCE = new NewCalcDistributionOperator();

    private NewCalcDistributionOperator() {
    }

    private static NavigableSet<RangeDistribution> getRangeDistributions(@NonNull DistributionSourceParam param) {
        PartitionService ps = param.getPs();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution
            = param.getRangeDistribution();
        LogUtils.trace(log,
            "start = {}, end = {}, PartitionService = {}, RangeDistribution = {}",
            Arrays.toString(param.getStartKey()),
            Arrays.toString(param.getEndKey()),
            ps.getClass().getCanonicalName(),
            rangeDistribution.entrySet().stream()
                .map(e -> e.getKey().encodeToString()+": "+e.getValue())
                .collect(Collectors.joining("\n"))
        );
        NavigableSet<RangeDistribution> distributions;
        if (param.getFilter() != null || param.isNotBetween()) {
            if (param.isLogicalNot() || param.isNotBetween()) {
                distributions = new TreeSet<>(RangeUtils.rangeComparator(1));
                distributions.addAll(ps.calcPartitionRange(
                    null,
                    param.getStartKey(),
                    true,
                    !param.isWithStart(),
                    param.getRangeDistribution()));
                distributions.addAll(ps.calcPartitionRange(
                    param.getEndKey(),
                    null,
                    !param.isWithEnd(),
                    true,
                    param.getRangeDistribution()));
                return distributions;
            }
        }
        return ps.calcPartitionRange(
            param.getStartKey(),
            param.getEndKey(),
            param.isWithStart(),
            param.isWithEnd(),
            rangeDistribution
        );
    }

    @Override
    public boolean push(Context context, @NonNull Vertex vertex) {
        DistributionSourceParam param = vertex.getParam();
        Set<RangeDistribution> distributions = getRangeDistributions(param);
        if (log.isTraceEnabled()) {
            if (distributions.isEmpty()) {
                LogUtils.trace(
                    log,
                    "No data distribution from ({}) to ({})",
                    Arrays.toString(param.getStartKey()),
                    Arrays.toString(param.getEndKey())
                );
            }
        }
        boolean parallel = Utils.parallel(param.getKeepOrder());
        if (!parallel || distributions.size() == 1) {
            for (RangeDistribution distribution : distributions) {
                if (log.isTraceEnabled()) {
                    LogUtils.trace(log, "Push distribution: {}", distribution);
                }
                context.setDistribution(distribution);
                if (!vertex.getSoleEdge().transformToNext(context, null)) {
                    break;
                }
            }
        } else {
            try {
                int concurrencyLevel = param.getConcurrencyLevel();
                Set<CompletableFuture<Boolean>> futures = new HashSet<>(concurrencyLevel);
                for (RangeDistribution distribution : distributions) {
                    CompletableFuture<Boolean> future = push(context, vertex, param, distribution);
                    futures.add(future);
                    if (futures.size() >= concurrencyLevel) {
                        // Wait for all the current futures to complete
                        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                        futures.clear();
                    }
                }
                // Wait for any remaining futures to complete
                if (!futures.isEmpty()) {
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                }
            } catch (CompletionException exception) {
                if (exception.getCause() instanceof LockWaitException) {
                    throw new LockWaitException("Lock wait");
                }
                throw exception;
            }
        }
        return false;
    }

    private static CompletableFuture<Boolean> push(
        Context context,
        Vertex vertex,
        DistributionSourceParam param,
        RangeDistribution distribution
    ) {
        Supplier<Boolean> supplier = () -> {
            if (log.isTraceEnabled()) {
                LogUtils.trace(log, "Push distribution: {}", distribution);
            }
            Context copyContext = context.copy();
            copyContext.setDistribution(distribution);
            return vertex.getSoleEdge().transformToNext(copyContext, null);
        };
        return CompletableFuture.supplyAsync(
            supplier, Executors.executor(
                "operator-" + vertex.getTask().getJobId() + "-"
                    + vertex.getTask().getId() + "-" + vertex.getId() + "-" + distribution.getId()))
            .exceptionally(ex -> {
                if (ex != null) {
                    if (ex.getCause() instanceof RegionSplitException) {
                        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> tmpDistribution =
                            MetaService.root().getRangeDistribution(param.getTd().getTableId());
                        DistributionSourceParam copyParam = param.copy(
                            tmpDistribution,
                            distribution.getStartKey(),
                            distribution.getEndKey(),
                            distribution.isWithStart(),
                            distribution.isWithEnd());
                        NavigableSet<RangeDistribution> rangeDistributions = getRangeDistributions(copyParam);
                        for (RangeDistribution rangeDistribution : rangeDistributions) {
                            push(context, vertex, param, rangeDistribution);
                        }
                    } else if (ex.getCause() instanceof LockWaitException) {
                        LogUtils.error(log, "jobId:" + vertex.getTask().getJobId() + ", taskId:"
                            + vertex.getTask().getId() + ", vertexId:" + vertex.getId() + ", error:", ex);
                        throw new LockWaitException("Lock wait");
                    } else {
                        throw new RuntimeException(ex);
                    }
                }
            return true;
        });
    }
}
