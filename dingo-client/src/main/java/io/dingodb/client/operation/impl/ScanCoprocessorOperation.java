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
import io.dingodb.client.common.KeyValueCodec;
import io.dingodb.client.common.Record;
import io.dingodb.client.common.TableInfo;
import io.dingodb.client.operation.Coprocessor;
import io.dingodb.client.operation.Coprocessor.SchemaWrapper;
import io.dingodb.client.operation.RangeUtils;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.utils.LinkedIterator;
import io.dingodb.sdk.common.utils.Parameters;

import java.util.*;
import java.util.stream.Collectors;

import static io.dingodb.client.operation.RangeUtils.*;

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
                    resultSchemas.add(buildColumnDefinition(column.getName(), column.getType(), i, column));
                }
            }
            keyRangeCoprocessor.aggregations.stream().map(agg -> {
                Column column = definition.getColumn(agg.columnName);
                return buildColumnDefinition(
                    Parameters.cleanNull(agg.alias, column.getName()),
                    agg.operation.resultType(Parameters.nonNull(column.getType(), "Agg type must non null.")),
                    -1,
                    column);
            }).forEach(resultSchemas::add);
            Coprocessor coprocessor = new Coprocessor(
                keyRangeCoprocessor.aggregations.stream().map(agg -> mapping(agg, definition)).collect(Collectors.toList()),
                new SchemaWrapper(tableInfo.tableId.entityId(), definition.getColumns()),
                new SchemaWrapper(tableInfo.tableId.entityId(), resultSchemas.stream().map(RangeUtils::mapping).collect(Collectors.toList())),
                groupBy.stream().map(definition::getColumnIndex).collect(Collectors.toList())
            );

            if (validateKeyRange(keyRange) && validateOpRange(range = convert(codec, definition, keyRange))) {
                subTasks = getSubTasks(tableInfo, range, coprocessor);
            }
            return new Fork(new Iterator[subTasks.size()], subTasks, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ColumnDefinition buildColumnDefinition(String name, String type, int primary, Column column) {
        return ColumnDefinition.getInstance(
            name,
            type,
            column.getElementType(),
            column.getPrecision(),
            column.getScale(),
            column.isNullable(),
            primary,
            column.getDefaultValue(),
            column.isAutoIncrement());
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
            coprocessor
        );

        List<Column> columnDefinitions = coprocessor.getResultSchema().getSchemas();
        KeyValueCodec codec = new KeyValueCodec(
            DingoKeyValueCodec.of(context.getTableId().entityId(), columnDefinitions), columnDefinitions
        );
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
