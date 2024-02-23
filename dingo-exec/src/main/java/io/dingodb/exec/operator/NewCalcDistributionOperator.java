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

import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.DistributionSourceParam;
import io.dingodb.partition.PartitionService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
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
        if (log.isTraceEnabled()) {
            log.trace(
                "start = {}, end = {}, PartitionService = {}, RangeDistribution = {}",
                Arrays.toString(param.getStartKey()),
                Arrays.toString(param.getEndKey()),
                ps.getClass().getCanonicalName(),
                rangeDistribution.entrySet().stream()
                    .map(e -> e.getKey().encodeToString()+": "+e.getValue())
                    .collect(Collectors.joining("\n"))
            );
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
        for (RangeDistribution distribution : distributions) {
            if (log.isTraceEnabled()) {
                log.trace("Push distribution: {}", distribution);
            }
            context.setDistribution(distribution);
            if (!vertex.getSoleEdge().transformToNext(context, null)) {
                break;
            }
        }
        return false;
    }
}
