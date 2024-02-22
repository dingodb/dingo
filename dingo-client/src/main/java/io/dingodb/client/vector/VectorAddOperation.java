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

import io.dingodb.client.common.ArrayWrapperList;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.service.entity.common.VectorWithId;
import io.dingodb.sdk.service.entity.index.VectorAddRequest;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.IndexDefinition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

public class VectorAddOperation implements Operation {

    private static final VectorAddOperation INSTANCE = new VectorAddOperation();

    public static VectorAddOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public Fork fork(Any parameters, Index indexInfo) {
        List<VectorWithId> vectors = parameters.getValue();
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparing(t -> t.getRegionId().getEntityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        IndexDefinition index = indexInfo.definition;

        long count = vectors.stream()
            .map(VectorWithId::getId)
            .peek(id -> {
                if (!index.isWithAutoIncrment() && id <= 0) {
                    throw new DingoClientException("Vector id must great than zero.");
                }
            })
            .distinct().count();
        if (!index.isWithAutoIncrment() && vectors.size() != count) {
            throw new DingoClientException(-1, "Vectors cannot be added repeatedly");
        }

        for (int i = 0; i < vectors.size(); i++) {
            VectorWithId vector = vectors.get(i);
            if (index.isWithAutoIncrment()) {
                long id = indexInfo.autoIncrementService.next(indexInfo.id);
                vector.setId(id);
            }
            byte[] key = VectorKeyCodec.encode(vector.getId());
            Map<VectorWithId, Integer> regionParams = subTaskMap.computeIfAbsent(
                indexInfo.partitions.lookup(key, vector.getId()), k -> new Any(new HashMap<>())
            ).getValue();

            regionParams.put(vector, i);

        }
        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(new VectorWithId[vectors.size()], subTasks, true);
    }

    @Override
    public Fork fork(OperationContext context, Index indexInfo) {
        Map<VectorWithId, Integer> parameters = context.parameters();
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparing(t -> t.getRegionId().getEntityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();
        for (Map.Entry<VectorWithId, Integer> parameter : parameters.entrySet()) {
            byte[] key = VectorKeyCodec.encode(parameter.getKey().getId());
            Map<VectorWithId, Integer> regionParams = subTaskMap.computeIfAbsent(
                indexInfo.partitions.lookup(key, parameter.getKey().getId()), k -> new Any(new HashMap<>())
            ).getValue();

            regionParams.put(parameter.getKey(), parameter.getValue());
        }

        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(context.result(), subTasks, true);
    }

    @Override
    public void exec(OperationContext context) {
        Map<VectorWithId, Integer> parameters = context.parameters();
        List<VectorWithId> vectors = new ArrayList<>(parameters.keySet());
        List<Boolean> result = context.getIndexService().vectorAdd(
            context.getRequestId(),
            VectorAddRequest.builder()
                .isUpdate(context.getVectorContext().isUpdate())
                .replaceDeleted(context.getVectorContext().isReplaceDeleted())
                .vectors(vectors)
                .build()
        ).getKeyStates();
        for (int i = 0; i < vectors.size(); i++) {
            context.<VectorWithId[]>result()[parameters.get(vectors.get(i))] = result.get(i) ? vectors.get(i) : null;
        }
    }

    @Override
    public <R> R reduce(Fork fork) {
        return (R) new ArrayWrapperList<>(fork.<VectorWithId[]>result());
    }
}
