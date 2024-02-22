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

import io.dingodb.client.common.VectorDistanceArray;
import io.dingodb.client.common.VectorSearch;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.service.entity.common.VectorWithDistance;
import io.dingodb.sdk.service.entity.index.VectorSearchRequest;
import io.dingodb.sdk.service.entity.index.VectorWithDistanceResult;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.RangeDistribution;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class VectorSearchOperation implements Operation {

    private static final VectorSearchOperation INSTANCE = new VectorSearchOperation();

    public static VectorSearchOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean stateful() {
        return false;
    }

    @Override
    public Fork fork(Any parameters, Index indexInfo) {
        VectorSearch vectorSearch = parameters.getValue();
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparing(t -> t.getRegionId().getEntityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        List<RangeDistribution> rangeDistributions = indexInfo.distributions;
        for (int i = 0; i < rangeDistributions.size(); i++) {
            RangeDistribution distribution = rangeDistributions.get(i);
            Map<DingoCommonId, RegionSearchTuple> regionParam = subTaskMap.computeIfAbsent(
                distribution.getId(), k -> new Any(new HashMap<>())
            ).getValue();

            List<VectorTuple<VectorSearch>> tuples = new ArrayList<>();
            for (int i1 = 0; i1 < vectorSearch.getVectors().size(); i1++) {
                tuples.add(new VectorTuple<>(i1, vectorSearch));
            }
            regionParam.put(distribution.getId(), new RegionSearchTuple(i, tuples));
        }
        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(new VectorDistanceArray[subTasks.size()][vectorSearch.getVectors().size()], subTasks, false);
    }

    @RequiredArgsConstructor
    static class RegionSearchTuple {
        private final int regionI;
        private final List<VectorTuple<VectorSearch>> vs;
    }

    @Override
    public void exec(OperationContext context) {
        Map<DingoCommonId, RegionSearchTuple> parameters = context.parameters();
        RegionSearchTuple tuple = parameters.get(context.getRegionId());
        VectorSearch vectorSearch = tuple.vs.get(0).value;
        List<VectorWithDistanceResult> results = context.getIndexService().vectorSearch(
            context.getRequestId(),
            VectorSearchRequest.builder()
                .parameter(vectorSearch.getParameter())
                .vectorWithIds(vectorSearch.getVectors())
                .build()
        ).getBatchResults();
        if (results == null) {
            return;
        }
        for (int i = 0; i < results.size(); i++) {
            VectorWithDistanceResult result = results.get(i);
            if (result == null) {
                continue;
            }
            List<VectorWithDistance> distanceList = result.getVectorWithDistances().stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
            context.<VectorDistanceArray[][]>result()[tuple.regionI][tuple.vs.get(i).key] =
                new VectorDistanceArray(distanceList);
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
                    if (arrays[i][i1] == null) {
                        continue;
                    }
                    vectorDistanceArray.addAll(arrays[i][i1].vectorWithDistances);
                }
            }
        }
        return (R) new ArrayList<>(map.values());
    }
}
