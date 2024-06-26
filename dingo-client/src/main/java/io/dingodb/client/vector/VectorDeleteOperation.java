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

import io.dingodb.client.VectorContext;
import io.dingodb.client.common.ArrayWrapperList;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.service.entity.index.VectorDeleteRequest;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

public class VectorDeleteOperation implements Operation {

    private static final VectorDeleteOperation INSTANCE = new VectorDeleteOperation();

    public static VectorDeleteOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public Fork fork(Any parameters, Index indexInfo, VectorContext context) {
        List<Long> ids = parameters.getValue();
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparing(t -> t.getRegionId().getEntityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        for (int i = 0; i < ids.size(); i++) {
            Long id = ids.get(i);
            if (id < 0) {
                id = 0L;
            }

            byte[] key = VectorKeyCodec.encode(id);
            Map<Long, Integer> regionParams = subTaskMap.computeIfAbsent(
                indexInfo.partitions.lookup(key, id),
                k -> new Any(new HashMap<>())
            ).getValue();

            regionParams.put(id, i);
        }
        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(new Boolean[ids.size()], subTasks, false);
    }

    @Override
    public Fork fork(OperationContext context, Index indexInfo) {
        Map<Long, Integer> parameters = context.parameters();
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparingLong(t -> t.getRegionId().getEntityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        for (Map.Entry<Long, Integer> parameter : parameters.entrySet()) {
            byte[] key = VectorKeyCodec.encode(parameter.getKey());
            Map<Long, Integer> regionParams = subTaskMap.computeIfAbsent(
                indexInfo.partitions.lookup(key, parameter.getKey()), k -> new Any(new HashMap<>())
            ).getValue();

            regionParams.put(parameter.getKey(), parameter.getValue());
        }

        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(context.result(), subTasks, false);
    }

    @Override
    public void exec(OperationContext context) {
        Map<Long, Integer> parameters = context.parameters();
        List<Long> ids = new ArrayList<>(parameters.keySet());
        List<Boolean> result = context.getIndexService().vectorDelete(
            context.getRequestId(),
            VectorDeleteRequest.builder().ids(new ArrayList<>(parameters.keySet())).build()
        ).getKeyStates();

        for (int i = 0; i < ids.size(); i++) {
            context.<Boolean[]>result()[parameters.get(ids.get(i))] = result.get(i);
        }
    }

    @Override
    public <R> R reduce(Fork fork) {
        return (R) new ArrayWrapperList<>(fork.<Boolean[]>result());
    }
}
