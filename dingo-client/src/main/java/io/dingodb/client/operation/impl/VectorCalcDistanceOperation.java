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
import io.dingodb.sdk.common.vector.VectorCalcDistance;
import io.dingodb.sdk.common.vector.VectorDistanceRes;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import static io.dingodb.sdk.common.utils.Any.wrap;

public class VectorCalcDistanceOperation implements Operation {

    private static final VectorCalcDistanceOperation INSTANCE = new VectorCalcDistanceOperation();

    public static VectorCalcDistanceOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public Fork fork(Any parameters, IndexInfo indexInfo) {
        VectorCalcDistance calcDistance = parameters.getValue();
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparing(t -> t.getRegionId().entityId()));
        Map<DingoCommonId, VectorCalcDistance> subTaskMap = new HashMap<>();

        try {
            byte[] bytes = indexInfo.codec.encodeKey(new Object[]{calcDistance.getVectorId()});
            subTaskMap.putIfAbsent(
                indexInfo.calcRegionId(bytes),
                calcDistance
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, wrap(v))));
        return new Fork(new VectorDistanceRes[subTasks.size()], subTasks, false);
    }

    @Override
    public void exec(OperationContext context) {
        VectorCalcDistance parameters = context.parameters();
        VectorDistanceRes distanceRes = context.getIndexService().vectorCalcDistance(
            context.getIndexId(),
            context.getRegionId(),
            parameters);

        context.<VectorDistanceRes[]>result()[0] = distanceRes;
    }

    @Override
    public <R> R reduce(Fork fork) {
        return (R) fork.<VectorDistanceRes[]>result()[0];
    }
}
