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
import io.dingodb.client.common.TableInfo;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.codec.CodecUtils;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.table.Column;
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
import java.util.stream.Collectors;

import static io.dingodb.client.utils.OperationUtils.mapKey;

public class GetOperation implements Operation {

    private static final GetOperation INSTANCE = new GetOperation(true);
    private static final GetOperation NOT_STANDARD_INSTANCE = new GetOperation(false);

    private GetOperation(boolean standard) {
        this.standard = standard;
    }

    public static GetOperation getInstance() {
        return INSTANCE;
    }

    public static GetOperation getNotStandardInstance() {
        return NOT_STANDARD_INSTANCE;
    }

    private final boolean standard;

    @Override
    public Operation.Fork fork(Any parameters, TableInfo tableInfo) {
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
            return new Fork(new Record[keys.size()], subTasks, false);
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
        ArrayList<byte[]> keys = new ArrayList<>(parameters.keySet());
        Map<byte[], KeyValue> result = new TreeMap<>(ByteArrayUtils::compare);
        DingoCommonId regionId = context.getRegionId();
        context.getStoreService().kvBatchGet(
            context.getTableId(),
            regionId,
            keys.stream()
                .map(k -> context.getCodec().resetPrefix(k, regionId.parentId()))
                .collect(Collectors.toList())
        ).forEach(kv -> {
            byte[] bytes = context.getCodec().resetPrefix(kv.getKey(), context.getTableId().entityId());
            result.put(bytes, new KeyValue(bytes, kv.getValue()));
        });
        for (int i = 0; i < keys.size(); i++) {
            KeyValue keyValue = result.get(keys.get(i));
            if (keyValue == null) {
                continue;
            }
            Object[] values;
            if (standard) {
                values = context.getCodec().decode(keyValue);
            } else {
                values = context.getCodec().getKeyValueCodec().decode(keyValue);
            }
            context.<Record[]>result()[parameters.get(keys.get(i))] = new Record(
                context.getTable().getColumns(), values
            );
        }
    }

    @Override
    public <R> R reduce(Fork fork) {
        return (R) Arrays.asList(fork.<Record[]>result());
    }
}
