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
import io.dingodb.client.common.Key;
import io.dingodb.client.common.Record;
import io.dingodb.client.common.RouteTable;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.utils.ByteArrayUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static io.dingodb.client.utils.OperationUtils.mapKey;

public class GetOperation implements Operation {

    private static final GetOperation INSTANCE = new GetOperation();

    private GetOperation() {

    }

    public static GetOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public Operation.Fork fork(Any parameters, Table table, RouteTable routeTable) {
        try {
            List<Key> keys = parameters.getValue();
            NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparingLong(t -> t.getRegionId().entityId()));
            Map<DingoCommonId, Any> subTaskMap = new HashMap<>();
            KeyValueCodec codec = routeTable.codec;
            for (int i = 0; i < keys.size(); i++) {
                // todo not support wrong order key
                Object[] mapKey = mapKey(table, keys.get(i));
                byte[] key = codec.encodeKey(mapKey);
                Map<byte[], Integer> regionParams = subTaskMap.computeIfAbsent(
                    routeTable.calcRegionId(key), k -> new Any(new HashMap<>())
                ).getValue();

                regionParams.put(key, i);
            }
            subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
            return new Fork(new Record[keys.size()], subTasks, true);
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
        try {
            Map<byte[], Integer> parameters = context.parameters();
            ArrayList<byte[]> keys = new ArrayList<>(parameters.keySet());
            Map<byte[], KeyValue> result = new TreeMap<>(ByteArrayUtils::compare);
            context.getStoreService().kvBatchGet(
                context.getTableId(),
                context.getRegionId(),
                keys
            ).forEach(kv -> result.put(kv.getKey(), kv));
            for (int i = 0; i < keys.size(); i++) {
                if (result.get(keys.get(i)) == null) {
                    continue;
                }
                context.<Record[]>result()[parameters.get(keys.get(i))] = new Record(
                    context.getTable().getColumns(), context.getCodec().decode(result.get(keys.get(i)))
                );
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <R> R reduce(Fork fork) {
        return (R) Arrays.asList(fork.<Record[]>result());
    }
}
