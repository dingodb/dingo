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
import io.dingodb.sdk.common.utils.Any;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

public class VectorGetIdOperation implements Operation {

    private final static VectorGetIdOperation INSTANCE = new VectorGetIdOperation();

    public static VectorGetIdOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public Fork fork(Any parameters, IndexInfo indexInfo) {
        Boolean isGetMin = parameters.getValue();
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparing(t -> t.getRegionId().entityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        indexInfo.rangeDistribution.values().forEach(r -> subTaskMap.computeIfAbsent(
            r.getId(), k -> new Any(isGetMin)
        ));

        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(new Long[1], subTasks, false);
    }

    @Override
    public void exec(OperationContext context) {
        Boolean isGetMin = context.<Boolean>parameters();
        Long result = context.getIndexService().vectorGetBoderId(
            context.getIndexId(),
            context.getRegionId(),
            isGetMin
        );

        context.<Long[]>result()[0] = result;
    }

    @Override
    public <R> R reduce(Fork fork) {
        return (R) fork.<Long[]>result()[0];
    }
}
