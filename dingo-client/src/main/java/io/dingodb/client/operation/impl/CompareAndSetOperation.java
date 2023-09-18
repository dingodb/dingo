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
import io.dingodb.client.common.Record;
import io.dingodb.client.common.TableInfo;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.KeyValueWithExpect;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.sdk.common.utils.ByteArrayUtils.ComparableByteArray;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static io.dingodb.client.utils.OperationUtils.checkParameters;
import static io.dingodb.common.util.ByteArrayUtils.SKIP_LONG_POS;

public class CompareAndSetOperation implements Operation {

    private static final CompareAndSetOperation INSTANCE = new CompareAndSetOperation(true);
    private static final CompareAndSetOperation NOT_STANDARD_INSTANCE = new CompareAndSetOperation(false);

    private CompareAndSetOperation(boolean standard) {
        this.standard = standard;
    }

    public static CompareAndSetOperation getInstance() {
        return INSTANCE;
    }

    public static CompareAndSetOperation getNotStandardInstance() {
        return NOT_STANDARD_INSTANCE;
    }

    @AllArgsConstructor
    public static class Parameter {
        public final List<Record> records;
        public final List<Record> expects;
    }

    private final boolean standard;

    @Override
    public Fork fork(Any parameters, TableInfo tableInfo) {
        try {
            Table definition = tableInfo.definition;
            Parameter parameter = parameters.getValue();
            NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparingLong(t -> t.getRegionId().entityId()));
            Map<DingoCommonId, Any> subTaskMap = new HashMap<>();
            List<Record> records = parameter.records;
            List<ComparableByteArray> checkList = null;

            if (standard) {
                checkList =  new ArrayList<>(records.size());
            }
            for (int i = 0; i < records.size(); i++) {
                Record record = records.get(i);
                Object[] values;

                if (standard) {
                    values = record.extractValues(
                        definition.getColumns().stream().map(Column::getName).collect(Collectors.toList())
                    );
                } else {
                    values = record.getDingoColumnValuesInOrder();
                }

                if (standard) {
                    checkParameters(definition, values);
                }

                KeyValue keyValue = tableInfo.codec.encode(values);

                ComparableByteArray key = new ComparableByteArray(keyValue.getKey(), SKIP_LONG_POS);

                if (standard) {
                    if (checkList.contains(key)) {
                        throw new IllegalArgumentException(
                            "Has duplicate key on [" + i + "] and [" + checkList.indexOf(key) + "]"
                        );
                    }
                    checkList.add(key);;
                }

                record = parameter.expects.get(i);
                if (standard) {
                    values = record.extractValues(
                        definition.getColumns().stream().map(Column::getName).collect(Collectors.toList())
                    );
                } else {
                    values = record.getDingoColumnValuesInOrder();
                }

                KeyValue expect = tableInfo.codec.encode(values);

                if (standard && !ByteArrayUtils.equal(keyValue.getKey(), expect.getKey())) {
                    throw new IllegalArgumentException("Key not equal on [" + i + "] record.");
                }

                Map<KeyValueWithExpect, Integer> regionParams = subTaskMap.computeIfAbsent(
                    tableInfo.calcRegionId(keyValue.getKey()), k -> new Any(new HashMap<>())
                ).getValue();

                regionParams.put(new KeyValueWithExpect(keyValue, expect.getValue()), i);
            }
            subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
            return new Fork(new Boolean[records.size()], subTasks, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fork fork(OperationContext context, TableInfo tableInfo) {
        Map<KeyValueWithExpect, Integer> parameters = context.parameters();
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparingLong(t -> t.getRegionId().entityId()));

        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();
        for (Map.Entry<KeyValueWithExpect, Integer> parameter : parameters.entrySet()) {

            Map<KeyValueWithExpect, Integer> regionParams =  subTaskMap.computeIfAbsent(
                tableInfo.calcRegionId(parameter.getKey().getKey()), k -> new Any(new HashMap<>())
            ).getValue();

            regionParams.put(parameter.getKey(), parameter.getValue());
        }
        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));

        return new Fork(context.result(), subTasks, true);
    }

    @Override
    public void exec(OperationContext context) {
        Map<KeyValueWithExpect, Integer> parameters = context.parameters();
        List<KeyValueWithExpect> keyValueWithExpects = new ArrayList<>(parameters.keySet());
        List<Boolean> result = context.getStoreService().kvBatchCompareAndSet(
            context.getTableId(),
            context.getRegionId(),
            keyValueWithExpects.stream()
                .map(kvExp -> new KeyValueWithExpect(
                    context.getCodec().resetPrefix(kvExp.getKey(), context.getRegionId().parentId()),
                    kvExp.getValue(),
                    kvExp.getExpect()))
                .collect(Collectors.toList()),
            false
        );
        for (int i = 0; i < keyValueWithExpects.size(); i++) {
            context.<Boolean[]>result()[parameters.get(keyValueWithExpects.get(i))] = result.get(i);
        }
    }

    @Override
    public <R> R reduce(Fork fork) {
        return (R) new ArrayWrapperList<>(fork.<Boolean[]>result());
    }
}
