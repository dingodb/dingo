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
import io.dingodb.client.common.Key;
import io.dingodb.client.common.TableInfo;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.codec.CodecUtils;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Any;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static io.dingodb.client.utils.OperationUtils.mapKey;

public class DeleteOperation implements Operation {

    private static final DeleteOperation INSTANCE = new DeleteOperation(true);
    private static final DeleteOperation NOT_STANDARD_INSTANCE = new DeleteOperation(false);

    private DeleteOperation(boolean standard) {
        this.standard = standard;
    }

    public static DeleteOperation getInstance() {
        return INSTANCE;
    }

    public static DeleteOperation getNotStandardInstance() {
        return NOT_STANDARD_INSTANCE;
    }

    private final boolean standard;

    @Override
    public Fork fork(Any parameters, TableInfo tableInfo) {
        try {
            Table definition = tableInfo.definition;
            List<Key> keys = parameters.getValue();
            NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparingLong(t -> t.getRegionId().entityId()));
            Map<DingoCommonId, Any> subTaskMap = new HashMap<>();
            KeyValueCodec codec = tableInfo.codec;
            List<Column> columns = definition.getColumns();
            List<Column> keyColumns = definition.getKeyColumns();
            List<Column> sortedKeyColumns = CodecUtils.sortColumns(keyColumns);
            for (int i = 0; i < keys.size(); i++) {
                Object[] dst = new Object[columns.size()];
                Key key = keys.get(i);
                Object[] src = key.getUserKey().toArray();
                byte[] keyBytes = codec.encodeKey(
                    mapKey(src, dst, columns, key.columnOrder ? keyColumns : sortedKeyColumns)
                );

                Map<byte[], Integer> regionParams = subTaskMap.computeIfAbsent(
                    tableInfo.calcRegionId(keyBytes), k -> new Any(new HashMap<>())
                ).getValue();

                regionParams.put(keyBytes, i);
            }
            subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
            return new Fork(new Boolean[keys.size()], subTasks, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fork fork(OperationContext context, TableInfo tableInfo) {
        Map<byte[], Integer> parameters = context.parameters();
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparingLong(t -> t.getRegionId().entityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        for (Map.Entry<byte[], Integer> parameter : parameters.entrySet()) {

            Map<byte[], Integer> regionParams = subTaskMap.computeIfAbsent(
                tableInfo.calcRegionId(parameter.getKey()), k -> new Any(new HashMap<>())
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
                keySet.stream()
                    .map(k -> context.getCodec().resetPrefix(k, context.getRegionId().parentId()))
                    .collect(Collectors.toList())
        );
        parameters.forEach((k, v) -> context.<Boolean[]>result()[v] = result.get(keySet.indexOf(k)));
    }

    @Override
    public <R> R reduce(Fork fork) {
        return (R) new ArrayWrapperList<>(fork.<Boolean[]>result());
    }

}
