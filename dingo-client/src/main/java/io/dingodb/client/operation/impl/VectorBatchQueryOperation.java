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
import io.dingodb.client.common.VectorWithId;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.utils.Any;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

public class VectorBatchQueryOperation implements Operation {

    private final static VectorBatchQueryOperation INSTANCE = new VectorBatchQueryOperation();

    public static VectorBatchQueryOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public Fork fork(Any parameters, IndexInfo indexInfo) {
        List<Long> ids = parameters.getValue();
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparing(t -> t.getRegionId().entityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        for (int i = 0; i < ids.size(); i++) {
            int finalI = i;
            indexInfo.rangeDistribution.values().forEach(r -> {
                Map<Long, Integer> regionParams = subTaskMap.computeIfAbsent(
                    r.getId(), k -> new Any(new HashMap<>())
                ).getValue();

                regionParams.put(ids.get(finalI), finalI);
            });
        }

        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(new VectorWithId[ids.size()], subTasks, false);
    }

    @Override
    public void exec(OperationContext context) {
        Map<Long, io.dingodb.sdk.common.vector.VectorWithId> result = new HashMap<>();
        Map<Long, Integer> parameters = context.parameters();
        List<Long> ids = new ArrayList<>(parameters.keySet());
        context.getIndexService().vectorBatchQuery(
            context.getIndexId(),
            context.getRegionId(),
            ids,
            context.getVectorContext().isWithoutVectorData(),
            context.getVectorContext().getSelectedKeys()
        ).forEach(v -> result.put(v.getId(), v));
        for (Long id : ids) {
            io.dingodb.sdk.common.vector.VectorWithId withId = result.get(id);
            if (withId == null) {
                continue;
            }
            context.<VectorWithId[]>result()[parameters.get(id)] = new VectorWithId(
                withId.getId(), withId.getVector(), withId.getScalarData()
            );
        }
    }

    @Override
    public <R> R reduce(Fork fork) {
        return (R) Arrays.asList(fork.<VectorWithId[]>result());
    }
}
