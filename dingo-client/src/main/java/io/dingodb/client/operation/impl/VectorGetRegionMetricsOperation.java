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

package io.dingodb.client.operation.impl;

import io.dingodb.client.OperationContext;
import io.dingodb.client.common.IndexInfo;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.vector.VectorIndexMetrics;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

public class VectorGetRegionMetricsOperation implements Operation {

    private static final VectorGetRegionMetricsOperation INSTANCE = new VectorGetRegionMetricsOperation();

    public static VectorGetRegionMetricsOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public Fork fork(Any parameters, IndexInfo indexInfo) {
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparing(t -> t.getRegionId().entityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        List<RangeDistribution> rangeDistributions = new ArrayList<>(indexInfo.rangeDistribution.values());
        for (int i = 0; i < rangeDistributions.size(); i++) {
            RangeDistribution distribution = rangeDistributions.get(i);
            Map<DingoCommonId, Integer> regionParam = subTaskMap.computeIfAbsent(
                distribution.getId(), k -> new Any(new HashMap<>())
            ).getValue();

            regionParam.put(distribution.getId(), i);
        }

        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(new VectorIndexMetrics[subTasks.size()], subTasks, false);
    }

    @Override
    public Fork fork(OperationContext context, IndexInfo indexInfo) {
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparing(t -> t.getRegionId().entityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        List<RangeDistribution> rangeDistributions = new ArrayList<>(indexInfo.rangeDistribution.values());
        for (int i = 0; i < rangeDistributions.size(); i++) {
            RangeDistribution distribution = rangeDistributions.get(i);
            Map<DingoCommonId, Integer> regionParam = subTaskMap.computeIfAbsent(
                distribution.getId(), k -> new Any(new HashMap<>())
            ).getValue();

            regionParam.put(distribution.getId(), i);
        }

        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(context.result(), subTasks, false);
    }

    @Override
    public void exec(OperationContext context) {
        Map<DingoCommonId, Integer> parameters = context.parameters();
        VectorIndexMetrics vectorIndexMetrics = context.getIndexService().vectorGetRegionMetrics(
            context.getIndexId(),
            context.getRegionId()
        );

        context.<VectorIndexMetrics[]>result()[parameters.get(context.getRegionId())] = vectorIndexMetrics;
    }

    @Override
    public <R> R reduce(Fork fork) {
        VectorIndexMetrics result = null;
        for (VectorIndexMetrics temp : fork.<VectorIndexMetrics[]>result()) {
            if (result == null) {
                result = temp;
                continue;
            }
            result = result.merge(temp);
        }
        return (R) result;
    }
}
