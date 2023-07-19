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
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.vector.VectorSearchParameter;
import io.dingodb.sdk.common.vector.VectorWithDistanceResult;
import io.dingodb.sdk.common.vector.VectorWithId;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
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
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        List<RangeDistribution> rangeDistributions = new ArrayList<>(indexInfo.rangeDistribution.values());
        for (int i = 0; i < rangeDistributions.size(); i++) {
            RangeDistribution distribution = rangeDistributions.get(i);
            Map<DingoCommonId, RegionSearchTuple> regionParam = subTaskMap.computeIfAbsent(
                distribution.getId(), k -> new Any(new HashMap<>())
            ).getValue();

            List<VectorTuple> tuples = new ArrayList<>();
            for (int i1 = 0; i1 < vectorSearch.getVectors().size(); i1++) {
                tuples.add(new VectorTuple(i1, vectorSearch));
            }
            regionParam.put(distribution.getId(), new RegionSearchTuple(i, tuples));
        }
        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(new VectorDistanceArray[subTasks.size()][vectorSearch.getVectors().size()], subTasks, false);
    }

    @RequiredArgsConstructor
    static class RegionSearchTuple {
        private final int regionI;
        private final List<VectorTuple> vs;
    }

    @RequiredArgsConstructor
    static class VectorTuple {
        private final int k;
        private final VectorSearch v;
    }

    @Override
    public void exec(OperationContext context) {
        Map<DingoCommonId, RegionSearchTuple> parameters = context.parameters();
        RegionSearchTuple tuple = parameters.get(context.getRegionId());
        VectorSearch vectorSearch = tuple.vs.get(0).v;
        List<io.dingodb.sdk.common.vector.VectorWithDistanceResult> results = context.getIndexService().vectorSearch(
            context.getIndexId(),
            context.getRegionId(),
            vectorSearch.getVectors().stream().map(v -> new VectorWithId(
                v.getId(),
                v.getVector(),
                v.getScalarData())).collect(Collectors.toList()),
            new VectorSearchParameter(
                vectorSearch.getParameter().getTopN(),
                vectorSearch.getParameter().isWithoutVectorData(),
                vectorSearch.getParameter().isWithScalarData(),
                vectorSearch.getParameter().getSelectedKeys(),
                vectorSearch.getParameter().getSearch(),
                vectorSearch.getParameter().isUseScalarFilter())
        );
        for (int i = 0; i < results.size(); i++) {
            VectorWithDistanceResult result = results.get(i);
            List<VectorWithDistance> distanceList = result.getWithDistance().stream()
                .map(d -> new VectorWithDistance(
                    d.getId(), d.getVector(), d.getScalarData(), d.getDistance(), d.getMetricType()))
                .collect(Collectors.toList());
            context.<VectorDistanceArray[][]>result()[tuple.regionI][tuple.vs.get(i).k] = new VectorDistanceArray(distanceList);
        }
    }

    @Override
    public <R> R reduce(Fork fork) {
        VectorDistanceArray[][] arrays = fork.result();
        Map<Integer, VectorDistanceArray> map = new HashMap<>();
        for (int i = 0; i < fork.getSubTasks().size(); i++) {
            for (int i1 = 0; i1 < arrays[i].length; i1++) {
                VectorDistanceArray vectorDistanceArray = map.get(i1);
                if (vectorDistanceArray == null) {
                    map.put(i1, arrays[i][i1]);
                } else {
                    vectorDistanceArray.addAll(arrays[i][i1].vectorWithDistances);
                }
            }
        }
        return (R) new ArrayList<>(map.values());
    }
}
