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

import io.dingodb.client.common.ArrayWrapperList;
import io.dingodb.client.common.Key;
import io.dingodb.client.OperationContext;
import io.dingodb.client.common.RouteTable;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.type.TupleMapping;
import io.dingodb.sdk.common.utils.Any;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

public class DeleteOperation implements Operation {

    private static final DeleteOperation INSTANCE = new DeleteOperation();

    private DeleteOperation() {

    }

    public static DeleteOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public Fork fork(Any parameters, Table table, RouteTable routeTable) {
        try {
            List<Key> keys = parameters.getValue();
            NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparingLong(t -> t.getRegionId().entityId()));
            Map<DingoCommonId, Any> subTaskMap = new HashMap<>();
            KeyValueCodec codec = routeTable.getCodec();
            TupleMapping keyMapping = table.getKeyMapping();
            for (int i = 0; i < keys.size(); i++) {
                Object[] mapKey = mapKey(table, keys.get(i), keyMapping);
                byte[] key = codec.encodeKey(mapKey);

                Map<byte[], Integer> regionParams = subTaskMap.computeIfAbsent(
                    routeTable.calcRegionId(key), k -> new Any(new HashMap<>())
                ).getValue();

                regionParams.put(key, i);
            }
            subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
            return new Fork(new Boolean[keys.size()], subTasks, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fork fork(OperationContext context, RouteTable routeTable) {
        Map<byte[], Integer> parameters = context.parameters();
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparingLong(t -> t.getRegionId().entityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        for (Map.Entry<byte[], Integer> parameter : parameters.entrySet()) {

            Map<byte[], Integer> regionParams = subTaskMap.computeIfAbsent(
                routeTable.calcRegionId(parameter.getKey()), k -> new Any(new HashMap<>())
            ).getValue();

            regionParams.put(parameter.getKey(), parameter.getValue());
        }
        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(context.result(), subTasks, true);
    }

    @Override
    public void exec(OperationContext context) {
        Map<byte[], Integer> parameters = context.parameters();
        List<byte[]> keySet = new ArrayList<>(parameters.keySet());
        List<Boolean> result = context.getStoreService().kvBatchDelete(
                context.getTableId(),
                context.getRegionId(),
                keySet
        );
        parameters.forEach((k, v) -> context.<Boolean[]>result()[v] = result.get(keySet.indexOf(k)));
    }

    @Override
    public <R> R reduce(Fork fork) {
        return (R) new ArrayWrapperList<>(fork.<Boolean[]>result());
    }

}
