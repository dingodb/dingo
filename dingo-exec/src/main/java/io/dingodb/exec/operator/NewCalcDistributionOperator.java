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
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.transaction.exception.LockWaitException;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
                distributions.addAll(ps.calcPartitionRange(null, param.getStartKey(), true, !param.isWithStart(), param.getRangeDistribution()));
                distributions.addAll(ps.calcPartitionRange(param.getEndKey(), null, !param.isWithEnd(), true, param.getRangeDistribution()));
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
                log.trace(
                    "No data distribution from ({}) to ({})",
                    Arrays.toString(param.getStartKey()),
                    Arrays.toString(param.getEndKey())
                );
            }
        }
        boolean parallel = Utils.parallel(param.getKeepOrder());
        //boolean rangePart = "range".equalsIgnoreCase(param.getTd().getPartitionStrategy());
        boolean rangePart = false;
        if (!parallel || distributions.size() == 1 || !rangePart) {
            for (RangeDistribution distribution : distributions) {
                if (log.isTraceEnabled()) {
                    log.trace("Push distribution: {}", distribution);
                }
                context.setDistribution(distribution);
                if (!vertex.getSoleEdge().transformToNext(context, null)) {
                    break;
                }
            }
        } else {
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                distributions.stream().map(distribution -> {
                    Callable<Boolean> callable = () -> {
                        if (log.isTraceEnabled()) {
                            log.trace("Push distribution: {}", distribution);
                        }
                        Context context1 = context.copy();
                        context1.setDistribution(distribution);
                        return vertex.getSoleEdge().transformToNext(context1, null);
                    };
                    return Executors.submit("newCalc", callable);
                }).toArray(CompletableFuture[]::new));
            try {
                allFutures.get();
            } catch (ExecutionException | InterruptedException e) {
                if (e.getMessage().contains("RegionSplitException")) {
                    throw new RegionSplitException("io.dingodb.sdk.common.DingoClientException$InvalidRouteTableException");
                } else if (e.getCause() instanceof LockWaitException) {
                    throw new LockWaitException("Lock wait");
                } else {
                    throw new RuntimeException(e);
                }
            }

        }
        return false;
    }
}
