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
import io.dingodb.client.common.VectorDistanceArray;
import io.dingodb.client.common.VectorSearch;
import io.dingodb.client.common.VectorWithDistance;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.vector.VectorSearchParameter;
import io.dingodb.sdk.common.vector.VectorWithId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class VectorSearchOperation implements Operation {

    private final static VectorSearchOperation INSTANCE = new VectorSearchOperation();

    public static VectorSearchOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public Fork fork(Any parameters, IndexInfo indexInfo) {
        VectorSearch vectorSearch = parameters.getValue();
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparing(t -> t.getRegionId().entityId()));
        Map< DingoCommonId, Any> subTaskMap = new HashMap<>();

        indexInfo.rangeDistribution.values().forEach(r -> {
            List<VectorSearch> regionParams = subTaskMap.computeIfAbsent(
                r.getId(), k -> new Any(new ArrayList<>())
            ).getValue();

            regionParams.add(vectorSearch);
        });

        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(new VectorDistanceArray[subTasks.size()], subTasks, false);
    }

    @Override
    public void exec(OperationContext context) {
        VectorSearch vectorSearch = context.<List<VectorSearch>>parameters().get(0);
        List<io.dingodb.sdk.common.vector.VectorWithDistance> distances = context.getIndexService().vectorSearch(
            context.getIndexId(),
            context.getRegionId(),
            new VectorWithId(
                vectorSearch.getVector().getId(),
                vectorSearch.getVector().getVector(),
                vectorSearch.getVector().getMetaData()),
            new VectorSearchParameter(
                vectorSearch.getParameter().getTopN(),
                vectorSearch.getParameter().isWithAllMetaData(),
                vectorSearch.getParameter().getSelectedKeys())
        );
        NavigableSet<VectorWithDistance> distanceList = distances.stream().map(d -> new VectorWithDistance(
            new io.dingodb.client.common.VectorWithId(d.getId(), d.getVector(), d.getMetaData()),
            d.getDistance()
        )).collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(VectorWithDistance::getDistance))));

        context.<VectorDistanceArray[]>result()[0] = new VectorDistanceArray(distanceList);
    }

    @Override
    public <R> R reduce(Fork context) {
        VectorDistanceArray distanceArray = new VectorDistanceArray(
            new TreeSet<>(Comparator.comparing(VectorWithDistance::getDistance)));
        Arrays.stream(context.<VectorDistanceArray[]>result()).forEach(v -> distanceArray.addAll(v.vectorWithDistances));
        return (R) distanceArray;
    }
}
