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
import io.dingodb.client.common.Record;
import io.dingodb.client.common.TableInfo;
import io.dingodb.client.operation.RangeUtils;
import io.dingodb.common.Coprocessor;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.utils.LinkedIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.stream.Collectors;

import static io.dingodb.client.operation.RangeUtils.convert;
import static io.dingodb.client.operation.RangeUtils.getSubTasks;
import static io.dingodb.client.operation.RangeUtils.mapping;
import static io.dingodb.client.operation.RangeUtils.validateKeyRange;
import static io.dingodb.client.operation.RangeUtils.validateOpRange;

public class ScanCoprocessorOperation implements Operation {

    private static final ScanCoprocessorOperation INSTANCE = new ScanCoprocessorOperation();

    private ScanCoprocessorOperation() {
    }

    public static ScanCoprocessorOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public Fork fork(Any parameters, TableInfo tableInfo) {
        try {
            KeyValueCodec codec = tableInfo.codec;
            NavigableSet<Task> subTasks = Collections.emptyNavigableSet();
            KeyRangeCoprocessor keyRangeCoprocessor = parameters.getValue();
            OpKeyRange keyRange = keyRangeCoprocessor.opKeyRange;
            OpRange range;
            Table definition = tableInfo.definition;
            List<String> groupBy = keyRangeCoprocessor.groupBy;
            List<ColumnDefinition> resultSchemas = new ArrayList<>();
            if (!groupBy.isEmpty()) {
                for (int i = 0; i < groupBy.size(); i++) {
                    Column column = definition.getColumn(groupBy.get(i));
                    resultSchemas.add(ColumnDefinition.getInstance(
                        column.getName(),
                        column.getType(),
                        column.getElementType(),
                        column.getPrecision(),
                        column.getScale(),
                        column.isNullable(),
                        i,
                        column.getDefaultValue(),
                        column.isAutoIncrement()));
                }
            }
            keyRangeCoprocessor.aggregationOperators.stream().map(agg -> {
                switch (agg.operation) {
                    case AGGREGATION_NONE:
                    case SUM:
                    case MAX:
                    case MIN:
                        return mapping(definition.getColumn(agg.columnName));
                    case COUNT:
                    case COUNTWITHNULL:
                        Column column = definition.getColumn(agg.columnName);
                        return ColumnDefinition.getInstance(
                            column.getName(),
                            "LONG",
                            column.getElementType(),
                            column.getPrecision(),
                            column.getScale(),
                            column.isNullable(),
                            column.getPrimary(),
                            column.getDefaultValue(),
                            column.isAutoIncrement());
                    default:
                        throw new IllegalStateException("Unexpected value: " + agg.operation);
                }
            }).forEach(resultSchemas::add);
            Coprocessor coprocessor = Coprocessor.builder()
                .originalSchema(
                    Coprocessor.SchemaWrapper.builder().schemas(
                            definition.getColumns()
                                .stream()
                                .map(RangeUtils::mapping)
                                .collect(Collectors.toList()))
                        .commonId(tableInfo.tableId.entityId())
                        .build())
                .resultSchema(
                    Coprocessor.SchemaWrapper.builder().schemas(resultSchemas)
                        .commonId(tableInfo.tableId.entityId())
                        .build())
                .aggregations(keyRangeCoprocessor.aggregationOperators.stream()
                    .map(agg -> mapping(agg, definition))
                    .collect(Collectors.toList()))
                .groupBy(groupBy.stream().map(definition::getColumnIndex).collect(Collectors.toList()))
                .build();

            if (validateKeyRange(keyRange) && validateOpRange(range = convert(codec, definition, keyRange))) {
                subTasks = getSubTasks(tableInfo, range, coprocessor);
            }
            return new Fork(new Iterator[subTasks.size()], subTasks, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fork fork(OperationContext context, TableInfo tableInfo) {
        OpRangeCoprocessor rangeCoprocessor = context.parameters();
        NavigableSet<Task> subTasks = getSubTasks(tableInfo, rangeCoprocessor, rangeCoprocessor.coprocessor);
        return new Fork(context.result(), subTasks, true);
    }

    @Override
    public void exec(OperationContext context) {
        OpRangeCoprocessor rangeCoprocessor = context.parameters();
        Coprocessor coprocessor = rangeCoprocessor.coprocessor;

        Iterator<KeyValue> scanResult = context.getStoreService().scan(
            context.getTableId(),
            context.getRegionId(),
            rangeCoprocessor.range,
            rangeCoprocessor.withStart,
            rangeCoprocessor.withEnd,
            new io.dingodb.client.operation.Coprocessor(coprocessor));

        List<Column> columnDefinitions = coprocessor.getResultSchema().getSchemas()
            .stream()
            .map(RangeUtils::mapping)
            .collect(Collectors.toList());
        KeyValueCodec codec = DingoKeyValueCodec.of(context.getTableId().entityId(), columnDefinitions);
        context.<Iterator<Record>[]>result()[context.getSeq()] = new RecordIterator(
            columnDefinitions, codec, scanResult
        );
    }

    @Override
    public <R> R reduce(Fork fork) {
        LinkedIterator<Record> result = new LinkedIterator<>();
        Arrays.stream(fork.<Iterator<Record>[]>result()).forEach(result::append);
        return (R) result;
    }

}
