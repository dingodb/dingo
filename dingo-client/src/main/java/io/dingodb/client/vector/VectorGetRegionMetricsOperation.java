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

package io.dingodb.client.vector;

import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.service.entity.common.VectorIndexMetrics;
import io.dingodb.sdk.service.entity.index.VectorGetRegionMetricsRequest;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.RangeDistribution;

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
    public boolean stateful() {
        return false;
    }

    @Override
    public Fork fork(Any parameters, Index indexInfo) {
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparing(t -> t.getRegionId().getEntityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        List<RangeDistribution> rangeDistributions = indexInfo.distributions;
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
    public void exec(OperationContext context) {
        Map<DingoCommonId, Integer> parameters = context.parameters();
        VectorIndexMetrics vectorIndexMetrics = context.getIndexService().vectorGetRegionMetrics(
            context.getRequestId(),
            VectorGetRegionMetricsRequest.builder().build()
        ).getMetrics();

        context.<VectorIndexMetrics[]>result()[parameters.get(context.getRegionId())] = vectorIndexMetrics;
    }

    @Override
    public <R> R reduce(Fork fork) {
        VectorIndexMetrics result = null;
        for (VectorIndexMetrics temp : fork.<VectorIndexMetrics[]>result()) {
            result = merge(result, temp);
        }
        return (R) result;
    }

    public VectorIndexMetrics merge(VectorIndexMetrics result, VectorIndexMetrics other) {
        if (result == null) {
            return other;
        }
        result.setVectorIndexType(other.getVectorIndexType());
        result.setCurrentCount(Long.sum(result.getCurrentCount(), other.getCurrentCount()));
        result.setDeletedCount(Long.sum(result.getDeletedCount(), other.getDeletedCount()));
        result.setMemoryBytes(Long.sum(result.getMemoryBytes(), other.getDeletedCount()));
        result.setMaxId(Long.max(result.getMaxId(), other.getMaxId()));
        result.setMinId(Long.min(result.getMinId(), other.getMinId()));
        return result;
    }
}
