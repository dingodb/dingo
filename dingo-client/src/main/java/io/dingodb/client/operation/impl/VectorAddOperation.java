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
import io.dingodb.client.common.ArrayWrapperList;
import io.dingodb.client.common.IndexInfo;
import io.dingodb.client.common.VectorWithId;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.index.Index;
import io.dingodb.sdk.common.utils.Any;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class VectorAddOperation implements Operation {

    private final static VectorAddOperation INSTANCE = new VectorAddOperation();

    public static VectorAddOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public Fork fork(Any parameters, IndexInfo indexInfo) {
        List<VectorWithId> vectors = parameters.getValue();
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparing(t -> t.getRegionId().entityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        Index index = indexInfo.index;

        for (int i = 0; i < vectors.size(); i++) {
            VectorWithId vector = vectors.get(i);
            if (index.isAutoIncrement()) {
                long id = indexInfo.autoIncrementService.next(indexInfo.indexId);
                vector.setId(id);
            }

            int finalI = i;
            indexInfo.rangeDistribution.values().forEach(r -> {
                Map<VectorWithId, Integer> regionParams = subTaskMap.computeIfAbsent(
                    r.getId(), k -> new Any(new HashMap<>())
                ).getValue();

                regionParams.put(vector, finalI);
            });
        }
        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(new VectorWithId[vectors.size()], subTasks, true);
    }

    @Override
    public void exec(OperationContext context) {
        boolean result = context.getIndexService().vectorAdd(
            context.getIndexId(),
            context.getRegionId(),
            context.<Map<VectorWithId, Integer>>parameters().keySet().stream()
                .map(integer -> new io.dingodb.sdk.common.vector.VectorWithId(
                    integer.getId(),
                    integer.getVector(),
                    integer.getMetaData()))
                .collect(Collectors.toList()),
            false,
            false
        );
        context.<Map<VectorWithId, Integer>>parameters().forEach((key, value) -> context.<VectorWithId[]>result()[value] = result ? key : null);
    }

    @Override
    public <R> R reduce(Fork fork) {
        return (R) new ArrayWrapperList<>(fork.<VectorWithId[]>result());
    }
}
