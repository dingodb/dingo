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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

public class VectorGetIdOperation implements Operation {

    private static final VectorGetIdOperation INSTANCE = new VectorGetIdOperation();

    public static VectorGetIdOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public Fork fork(Any parameters, IndexInfo indexInfo) {
        Boolean isGetMin = parameters.getValue();
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparing(t -> t.getRegionId().entityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        List<RangeDistribution> rangeDistributions = new ArrayList<>(indexInfo.rangeDistribution.values());
        for (int i = 0; i < rangeDistributions.size(); i++) {
            RangeDistribution distribution = rangeDistributions.get(i);
            Map<DingoCommonId, VectorTuple<Boolean>> regionParam = subTaskMap.computeIfAbsent(
                distribution.getId(), k -> new Any(new HashMap<>())
            ).getValue();

            regionParam.put(distribution.getId(), new VectorTuple<>(i, isGetMin));
        }

        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(new long[subTasks.size()], subTasks, false);
    }

    @Override
    public Fork fork(OperationContext context, IndexInfo indexInfo) {
        Map<DingoCommonId, VectorTuple<Boolean>> parameters = context.parameters();
        Boolean isGetMin = new ArrayList<>(parameters.values()).get(0).value;
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparing(t -> t.getRegionId().entityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        List<RangeDistribution> rangeDistributions = new ArrayList<>(indexInfo.rangeDistribution.values());
        for (int i = 0; i < rangeDistributions.size(); i++) {
            RangeDistribution distribution = rangeDistributions.get(i);
            Map<DingoCommonId, VectorTuple<Boolean>> regionParams = subTaskMap.computeIfAbsent(
                distribution.getId(), k -> new Any(new HashMap<>())
            ).getValue();

            regionParams.put(distribution.getId(), new VectorTuple<>(i, isGetMin));
        }

        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(new long[subTasks.size()], subTasks, false);
    }

    @Override
    public void exec(OperationContext context) {
        Map<DingoCommonId, VectorTuple<Boolean>> parameters = context.parameters();
        Long result = context.getIndexService().vectorGetBoderId(
            context.getIndexId(),
            context.getRegionId(),
            parameters.get(context.getRegionId()).value
        );

        context.<long[]>result()[parameters.get(context.getRegionId()).key] = result;
    }

    @Override
    public <R> R reduce(Fork fork) {
        return (R) fork.<long[]>result();
    }
}
