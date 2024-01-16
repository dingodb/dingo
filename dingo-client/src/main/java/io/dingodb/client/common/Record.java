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

package io.dingodb.client.common;

import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.store.proxy.common.Mapping;
import io.dingodb.store.proxy.mapper.Mapper;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.math.RoundingMode.HALF_UP;

/**
 * Container object for records. Records are equivalent to rows.
 */
@Slf4j
@EqualsAndHashCode
public final class Record {

    private final Map<String, Integer> columnIndex = new HashMap<>();
    @Getter
    private final Column[] columns;
    private final Value[] values;

    public Record(List<Column> columns, List<Object> values) {
        this(columns, values.toArray());
    }

    public Record(List<Column> columns, Object[] values) {
        this(columns.toArray(new Column[columns.size()]), Stream.of(values).map(Value::get).toArray(Value[]::new));
    }

    public Record(Object[] values, List<io.dingodb.meta.entity.Column> columns) {
        this(columns.stream().map(Mapping::mapping).toArray(Column[]::new),
            Stream.of(values).map(Value::get).toArray(Value[]::new));
    }

    public Record(Column[] columns, Object[] values) {
        this(columns, Stream.of(values).map(Value::get).toArray(Value[]::new));
    }

    public Record(Column[] columns, Value[] values) {
        this.columns = columns;
        this.values = values;
        for (int i = 0; i < columns.length; i++) {
            columnIndex.put(columns[i].getName(), i);
        }
    }

    @Deprecated
    public Record(final String keyColumn, final LinkedHashMap<String, Object> valueMap) {
        this(Collections.singletonList(keyColumn), valueMap);
    }

    @Deprecated
    public Record(final List<String> keyColumns, final LinkedHashMap<String, Object> valueMap) {
        int index = 0;
        int primary = 0;
        columns = new Column[valueMap.size()];
        values = new Value[valueMap.size()];
        for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
            ColumnDefinition column = ColumnDefinition.builder()
                .name(entry.getKey().toUpperCase())
                .primary(keyColumns.contains(entry.getKey()) ? primary++ : -1)
                .build();
            Value value = Value.get(entry.getValue());
            columns[index] = column;
            values[index] = value;
            index++;
        }
        for (int i = 0; i < columns.length; i++) {
            columnIndex.put(columns[i].getName(), i);
        }
    }

    public Object[] getDingoColumnValuesInOrder() {
        for (int i = 0; i < columns.length; i++) {
            if (values[i].getObject() == null) {
                continue;
            }
            switch (columns[i].getType().toUpperCase()) {
                case "DOUBLE":
                    this.values[i] = Value.get(new BigDecimal(
                        String.valueOf(this.values[i].getObject()))
                        .setScale(/*columns[i].getScale()*/2, HALF_UP).doubleValue());
                    break;
                case "FLOAT":
                    this.values[i] = Value.get(new BigDecimal(
                        String.valueOf(this.values[i].getObject()))
                        .setScale(columns[i].getScale(), HALF_UP).floatValue());
                    break;
                default:
                    continue;
            }
        }
        return Stream.of(values).map(Value::getObject).toArray(Object[]::new);
    }

    public List<String> getKeyColumnNames() {
        return Stream.of(columns)
            .filter(Column::isPrimary)
            .sorted(Comparator.comparingInt(Column::getPrimary))
            .map(Column::getName)
            .collect(Collectors.toList());
    }

    public void setValue(Object value, int index) {
        values[index] = Value.get(value);
    }

    public List<Object> getValues() {
        return Stream.of(values).map(Value::getObject).collect(Collectors.toList());
    }

    public List<Object> getValues(List<String> cols) {
        return cols.stream().map(this::getValue).collect(Collectors.toList());
    }

    public List<Object> getColumnValuesInOrder() {
        return Stream.of(values).map(Value::getObject).collect(Collectors.toList());
    }

    public Object[] extractValues(List<String> cols) {
        Object[] values = new Object[cols.size()];
        int index;
        for (int i = 0; i < cols.size(); i++) {
            String col = cols.get(i).toUpperCase();
            index = Parameters.check(columnIndex.get(col), Objects::nonNull,
                () -> new DingoClientException("column name: " + col + " that does not exist")
            );
            values[i] = this.values[index].getObject();
        }
        return values;
    }

    public Record extract(List<String> cols) {
        Column[] columns = new Column[cols.size()];
        Value[] values = new Value[cols.size()];
        int index;
        for (int i = 0; i < cols.size(); i++) {
            String col = cols.get(i).toUpperCase();
            index = Parameters.check(columnIndex.get(col), Objects::nonNull,
                () -> new DingoClientException("column name: " + col + " that does not exist")
            );
            columns[i] = this.columns[index];
            values[i] = this.values[index];
        }
        return new Record(columns, values);
    }

    /**
     * Get column value given column name.
     * @param name name
     * @return T
     * @param <T> T
     */
    public <T> T getValue(String name) {
        return (T) Optional.ofNullable(columnIndex.get(name)).map(i -> values[i].getObject()).orNull();
    }

    /**
     * Get column value as String.
     * @param name name
     * @return string
     */
    public String getString(String name) {
        return getValue(name);
    }

    /**
     * Get column value as double.
     * @param name name
     * @return double
     */
    public double getDouble(String name) {
        // The server may return number as double or long.
        // Convert bits if returned as long.
        Object result = getValue(name);
        return (result instanceof Double)
               ? (Double) result
               : (result != null) ? Double.longBitsToDouble((Long) result) : 0.0;
    }

    /**
     * Get column value as float.
     * @param name name
     * @return float
     */
    public float getFloat(String name) {
        return getValue(name);
    }

    /**
     * Get column value as long.
     * @param name name
     * @return long
     */
    public long getLong(String name) {
        // The server always returns numbers as longs if column found.
        // If column not found, the result will be null.  Convert null to zero.
        Object result = getValue(name);
        return (result != null) ? (Long) result : 0;
    }

    /**
     * Get column value as int.
     * @param name name
     * @return int
     */
    public int getInt(String name) {
        // The server always returns numbers as longs, so get long and cast.
        return (int) getLong(name);
    }

    /**
     * Get column value as short.
     * @param name name
     * @return short
     */
    public short getShort(String name) {
        return (short) getLong(name);
    }

    /**
     * Get column value as byte.
     * @param name name
     * @return byte
     */
    public byte getByte(String name) {
        return (byte) getLong(name);
    }

    /**
     * Get column value as boolean.
     * @param name name
     * @return boolean
     */
    public boolean getBoolean(String name) {
        Object result = getValue(name);

        if (result instanceof Boolean) {
            return (Boolean) result;
        }

        if (result != null) {
            long v = (Long) result;
            return v != 0;
        }
        return false;
    }

    /**
     * Get column value as list.
     * @param name name
     * @return list
     */
    public List<?> getList(String name) {
        return getValue(name);
    }

    /**
     * Get column value as map.
     * @param name name
     * @return map
     */
    public Map<?, ?> getMap(String name) {
        return getValue(name);
    }

    @Override
    public String toString() {
        StringJoiner stringJoiner = new StringJoiner(", ", Record.class.getSimpleName() + "[", "]");
        for (int i = 0; i < columns.length; i++) {
            stringJoiner.add(columns[i].getName() + "=" + values[i]);
        }
        return stringJoiner.toString();
    }
}
