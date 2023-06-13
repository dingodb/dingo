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

import com.google.common.collect.Iterators;
import io.dingodb.client.OperationContext;
import io.dingodb.client.common.KeyValueCodec;
import io.dingodb.client.common.Record;
import io.dingodb.client.common.TableInfo;
import io.dingodb.client.operation.Coprocessor;
import io.dingodb.client.operation.Coprocessor.SchemaWrapper;
import io.dingodb.client.operation.RangeUtils;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.utils.Parameters;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
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
            List<KeyRangeCoprocessor.Aggregation> aggregations = keyRangeCoprocessor.aggregations;
            List<String> aggrAliases = new ArrayList<>();
            for (int i = 0; i < aggregations.size(); i++) {
                KeyRangeCoprocessor.Aggregation agg = aggregations.get(i);
                Parameters.nonNull(agg.operation, "Aggregation [" + i + "] operation is null.");
                Parameters.nonNull(agg.columnName, "Aggregation [" + i + "] column is null.");
                Column column = Parameters.nonNull(
                    definition.getColumn(agg.columnName),
                    "Aggregation [" + i + "] column [" + agg.columnName + "] not found."
                );
                String alias = agg.alias;
                if (alias == null) {
                    alias = "_" + agg.operation + "_" + agg.columnName + "_" + i + "_";
                    while (aggrAliases.contains(alias)) {
                        alias += "$";
                    }
                }
                if (aggrAliases.contains(alias)) {
                    throw new IllegalArgumentException("Has duplicate aggregation alias");
                }
                aggrAliases.add(alias);
                resultSchemas.add(buildColumnDefinition(alias, agg.operation.resultType(column.getType()), -1, column));
            }
            Coprocessor coprocessor = new Coprocessor(
                aggregations.stream().map(agg -> mapping(agg, definition)).collect(Collectors.toList()),
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
        context.<Iterator<KeyValue>[]>result()[context.getSeq()] = new CoprocessorIterator(
            columnDefinitions, codec, scanResult
        );
    }

    @Override
    public <R> R reduce(Fork fork) {

        List<KeyValue> list = new ArrayList<>();
        Map<ByteArrayUtils.ComparableByteArray, Record> cache = new ConcurrentHashMap<>();
        Arrays.stream(fork.<Iterator<KeyValue>[]>result()).forEach(iter -> iter.forEachRemaining(list::add));

        NavigableSet<Task> subTasks = fork.getSubTasks();
        Coprocessor coprocessor = subTasks.pollLast().<OpRangeCoprocessor>parameters().coprocessor;
        List<Column> columnDefinitions = coprocessor.getResultSchema().getSchemas();
        List<io.dingodb.sdk.service.store.AggregationOperator> aggregations = coprocessor.getAggregations();

        KeyValueCodec codec = ((CoprocessorIterator) fork.<Iterator<KeyValue>[]>result()[0]).getCodec();

        List<Column> resultSchemas = coprocessor.resultSchema.getSchemas();
        for (KeyValue record : list) {
            try {
                Record current = new Record(columnDefinitions, codec.getKeyValueCodec().decode(record));
                ByteArrayUtils.ComparableByteArray byteArray = new ByteArrayUtils.ComparableByteArray(record.getKey());
                if (cache.get(byteArray) == null) {
                    cache.put(byteArray, current);
                    continue;
                } else {
                    for (int i = 1; i <= aggregations.size(); i++) {
                        Record old = cache.get(byteArray);
                        Object result = reduce(
                            (KeyRangeCoprocessor.AggType) aggregations.get(aggregations.size() - i).getOperation(),
                            current.getValues().get(current.getValues().size() - i),
                            old.getValues().get(old.getValues().size() - i),
                            resultSchemas.get(resultSchemas.size() - i));
                        current.setValue(result, current.getValues().size() - i);
                    }
                }
                cache.put(byteArray, current);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return (R) Iterators.transform(cache.values().iterator(),
            r -> new Record(r.getColumns(),
                (Object[]) codec.getDingoType().convertFrom(r.getDingoColumnValuesInOrder(), DingoConverter.INSTANCE)));
    }

    private static Object reduce(KeyRangeCoprocessor.AggType operation, Object current, Object old, Column column) {
        Object value;
        switch (operation) {
            case SUM:
            case SUM0:
            case COUNT:
            case COUNT_WITH_NULL:
                BigDecimal currentDecimal = new BigDecimal(String.valueOf(current));
                BigDecimal oldDecimal = new BigDecimal(String.valueOf(old));
                switch (column.getType().toUpperCase()) {
                    case "INTEGER":
                        value = currentDecimal.add(oldDecimal).intValue();
                        break;
                    case "LONG":
                    case "BIGINT":
                        value = currentDecimal.add(oldDecimal).longValue();
                        break;
                    case "DOUBLE":
                        value = currentDecimal.add(oldDecimal).doubleValue();
                        break;
                    case "FLOAT":
                        value = currentDecimal.add(oldDecimal).floatValue();
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + column.getType().toUpperCase());
                }
                break;
            case MAX:
                value = ((Comparable) current).compareTo(old) > 0 ? current : old;
                break;
            case MIN:
                value = ((Comparable) old).compareTo(current) > 0 ? current : old;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + operation);
        }

        return value;


    }

}
