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
import io.dingodb.sdk.common.Range;
import io.dingodb.sdk.common.codec.CodecUtils;
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

import static io.dingodb.client.operation.RangeUtils.convert;
import static io.dingodb.client.operation.RangeUtils.getSubTasks;
import static io.dingodb.client.operation.RangeUtils.mapping;
import static io.dingodb.client.operation.RangeUtils.validateKeyRange;
import static io.dingodb.client.operation.RangeUtils.validateOpRange;
import static java.math.RoundingMode.HALF_UP;

public class ScanCoprocessorOperation implements Operation {

    private static final ScanCoprocessorOperation INSTANCE = new ScanCoprocessorOperation(true);
    private static final ScanCoprocessorOperation NOT_STANDARD_INSTANCE = new ScanCoprocessorOperation(false);

    private ScanCoprocessorOperation(boolean standard) {
        this.standard = standard;
    }

    public static ScanCoprocessorOperation getInstance() {
        return INSTANCE;
    }

    public static ScanCoprocessorOperation getNotStandardInstance() {
        return NOT_STANDARD_INSTANCE;
    }

    private final boolean standard;

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
                if (standard && !agg.operation.checkType(column.getType())) {
                    throw new IllegalArgumentException("Unsupported " + agg.operation + " " + column.getType());
                }
                aggrAliases.add(alias);
                resultSchemas.add(buildColumnDefinition(alias, agg.operation.resultType(column.getType()), -1, column));
            }
            Coprocessor coprocessor = new Coprocessor(
                aggregations.stream().map(agg -> mapping(agg, definition)).collect(Collectors.toList()),
                new SchemaWrapper(tableInfo.tableId.entityId(), definition.getColumns()),
                new SchemaWrapper(tableInfo.tableId.entityId(), resultSchemas.stream()
                    .map(RangeUtils::mapping)
                    .collect(Collectors.toList())),
                groupBy.stream().map(definition::getColumnIndex).collect(Collectors.toList())
            );

            if (validateKeyRange(keyRange) && validateOpRange(range = convert(codec, definition, keyRange))) {
                subTasks = getSubTasks(tableInfo, range, coprocessor);
            }
            return new Fork(new Iterator[subTasks.size()], subTasks, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fork fork(OperationContext context, TableInfo tableInfo) {
        return null;
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
            column.isAutoIncrement(),
            column.getState());
    }

    @Override
    public void exec(OperationContext context) {
        OpRangeCoprocessor rangeCoprocessor = context.parameters();
        Coprocessor coprocessor = rangeCoprocessor.coprocessor;

        Range range = rangeCoprocessor.range;
        Iterator<KeyValue> scanResult = context.getStoreService().scan(
            context.getTableId(),
            context.getRegionId(),
            new Range(context.getCodec().resetPrefix(range.getStartKey(), context.getRegionId().parentId()), range.getEndKey()),
            rangeCoprocessor.withStart,
            rangeCoprocessor.withEnd,
            coprocessor
        );

        List<Column> columnDefinitions = coprocessor.getResultSchema().getSchemas();
        KeyValueCodec codec = new KeyValueCodec(
            DingoKeyValueCodec.of(context.getTableId().entityId(), columnDefinitions), columnDefinitions
        );
        context.<Iterator<KeyValue>[]>result()[context.getSeq()] = new CoprocessorIterator(
            columnDefinitions, codec, scanResult, context.getTableId().entityId()
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
        }

        if (standard) {
            return (R) Iterators.transform(cache.values().iterator(), r -> new Record(
                r.getColumns(),
                (Object[]) codec.getDingoType().convertFrom(r.getDingoColumnValuesInOrder(), DingoConverter.INSTANCE))
            );
        } else {
            return (R) cache.values().iterator();
        }
    }

    private static Object reduce(KeyRangeCoprocessor.AggType operation, Object current, Object old, Column column) {
        if (current == null) {
            return old;
        }
        if (old == null) {
            return current;
        }
        Object value;
        switch (operation) {
            case SUM:
            case SUM0:
            case COUNT:
            case COUNT_WITH_NULL:
                BigDecimal currentDecimal = new BigDecimal(String.valueOf(current));
                BigDecimal oldDecimal = new BigDecimal(String.valueOf(old));
                switch (CodecUtils.createSchemaForColumn(column).getType()) {
                    case INTEGER:
                        value = currentDecimal.add(oldDecimal).intValue();
                        break;
                    case LONG:
                        value = currentDecimal.add(oldDecimal).longValue();
                        break;
                    case DOUBLE:
                        currentDecimal = currentDecimal.add(oldDecimal);
                        if (column.getScale() > 0) {
                            currentDecimal = currentDecimal.setScale(column.getScale(), HALF_UP);
                        }
                        value = currentDecimal.doubleValue();
                        break;
                    case FLOAT:
                        currentDecimal = currentDecimal.add(oldDecimal);
                        if (column.getScale() > 0) {
                            currentDecimal = currentDecimal.setScale(column.getScale(), HALF_UP);
                        }
                        value = currentDecimal.floatValue();
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
