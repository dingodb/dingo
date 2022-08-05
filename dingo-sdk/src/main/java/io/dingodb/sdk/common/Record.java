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

package io.dingodb.sdk.common;

import com.google.common.collect.Maps;
import io.dingodb.common.operation.Column;
import io.dingodb.common.operation.Value;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.sdk.annotation.DingoColumn;
import io.dingodb.sdk.annotation.DingoKey;
import io.dingodb.sdk.utils.DingoClientException;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Container object for records.
 * Records are equivalent to rows.
 */
@Slf4j
public final class Record {
    /**
     * Map of requested name/value columns.
     * The value is Java original type is not `Value` type.
     */
    private final Map<String, Object> columnsInOrderMapping;

    private final List<String> keyColumns;


    /**
     * Constructor Record by TableDefinition and columns.
     * @param columnDefinitions defined in table definition(sequential as create table).
     * @param inputColumns   real record columns(the column order is not important)
     */
    public Record(final List<ColumnDefinition> columnDefinitions, final Column[] inputColumns) {
        keyColumns = new ArrayList<>();
        columnsInOrderMapping = Maps.newLinkedHashMap();

        if (columnDefinitions.size() != inputColumns.length) {
            String errMsg = "Input Column Count mismatch. Table of Meta:"
                + columnDefinitions.size() + ", input real:" + inputColumns.length;
            throw new DingoClientException(errMsg);
        }


        for (ColumnDefinition columnDefinition: columnDefinitions) {
            String columnName = columnDefinition.getName();
            if (columnDefinition.isPrimary()) {
                keyColumns.add(columnName);
            }

            for (Column column: inputColumns) {
                if (Objects.equals(column.name, columnName)) {
                    columnsInOrderMapping.put(columnName, column.value.getObject());
                    break;
                }
            }
        }
    }

    /**
     * Constructor a record from a object instance.
     * @param instance input object value
     */
    public Record(Object instance) {
        keyColumns = new ArrayList<>();
        columnsInOrderMapping = Maps.newLinkedHashMap();

        for (Field field: instance.getClass().getDeclaredFields()) {
            String fieldName = field.getName();

            if (field.isAnnotationPresent(DingoKey.class)) {
                keyColumns.add(fieldName);
            }

            if (field.isAnnotationPresent(DingoColumn.class)) {
                String localFieldName = field.getAnnotation(DingoColumn.class).name();
                boolean isOK = localFieldName != null && !localFieldName.isEmpty();
                if (isOK) {
                    fieldName = localFieldName;
                }
            }

            try {
                field.setAccessible(true);
                Object value = field.get(instance);
                columnsInOrderMapping.put(fieldName, value);
            } catch (IllegalAccessException e) {
                log.warn("Visit table columns using reflection failed.", e);
                throw new RuntimeException(e);
            }
        }
    }

    public Record(final List<String> keyColumns, final Map<String, Object> columns) {
        this.keyColumns = new ArrayList<>(keyColumns.size());
        this.columnsInOrderMapping = Maps.newLinkedHashMap();

        boolean isValid = true;
        for (String keyColumn: keyColumns) {
            if (!columns.containsKey(keyColumn)) {
                isValid = false;
                break;
            }
        }

        if (!isValid) {
            log.error("Key columns:{} are not in the record:{}.",
                keyColumns, columns.keySet());
            throw new DingoClientException("Key columns are not in the record.");
        }

        for (String key: keyColumns) {
            this.keyColumns.add(key);
        }

        for (Map.Entry<String, Object> entry: columns.entrySet()) {
            this.columnsInOrderMapping.put(entry.getKey(), entry.getValue());
        }
    }

    public Object[] getDingoColumnValuesInOrder() {
        Object[] values = new Object[columnsInOrderMapping.size()];
        int i = 0;
        for (Map.Entry<String, Object> entry: columnsInOrderMapping.entrySet()) {
            values[i] = entry.getValue();
            i++;
        }
        return values;
    }

    public static Record toDingoRecord(Record record) {
        List<String> keyColumns = new ArrayList<>();
        for (String key: record.getKeyColumnNames()) {
            keyColumns.add(key);
        }

        Map<String, Object> columnsInOrder = new HashMap<String, Object>();
        for (Map.Entry<String, Object> entry: record.columnsInOrderMapping.entrySet()) {
            Object value = entry.getValue();
            if (entry.getValue() instanceof Value) {
                value = ((Value) entry.getValue()).getObject();
            }
            columnsInOrder.put(entry.getKey(), value);
        }
        return new Record(keyColumns, columnsInOrder);
    }

    public List<String> getKeyColumnNames() {
        return keyColumns;
    }

    public List<Object> getColumnValuesInOrder() {
        List<Object> values = new ArrayList<>();
        for (Map.Entry<String, Object> entry: columnsInOrderMapping.entrySet()) {
            values.add(entry.getValue());
        }
        return values;
    }

    /**
     * Get column value given column name.
     */
    public Object getValue(String name) {
        return (columnsInOrderMapping == null) ? null : columnsInOrderMapping.get(name);
    }

    /**
     * Get column value as String.
     */
    public String getString(String name) {
        return (String)getValue(name);
    }

    /**
     * Get column value as double.
     */
    public double getDouble(String name) {
        // The server may return number as double or long.
        // Convert bits if returned as long.
        Object result = getValue(name);
        return (result instanceof Double)
            ? (Double)result : (result != null) ? Double.longBitsToDouble((Long)result) : 0.0;
    }

    /**
     * Get column value as float.
     */
    public float getFloat(String name) {
        return (float)getDouble(name);
    }

    /**
     * Get column value as long.
     */
    public long getLong(String name) {
        // The server always returns numbers as longs if column found.
        // If column not found, the result will be null.  Convert null to zero.
        Object result = getValue(name);
        return (result != null) ? (Long)result : 0;
    }

    /**
     * Get column value as int.
     */
    public int getInt(String name) {
        // The server always returns numbers as longs, so get long and cast.
        return (int)getLong(name);
    }

    /**
     * Get column value as short.
     */
    public short getShort(String name) {
        return (short)getLong(name);
    }

    /**
     * Get column value as byte.
     */
    public byte getByte(String name) {
        return (byte)getLong(name);
    }

    /**
     * Get column value as boolean.
     */
    public boolean getBoolean(String name) {
        Object result = getValue(name);

        if (result instanceof Boolean) {
            return (Boolean)result;
        }

        if (result != null) {
            long v = (Long)result;
            return v != 0;
        }
        return false;
    }

    /**
     * Get column value as list.
     */
    public List<?> getList(String name) {
        return (List<?>)getValue(name);
    }

    /**
     * Get column value as map.
     */
    public Map<?,?> getMap(String name) {
        return (Map<?,?>)getValue(name);
    }

    /**
     * Return String representation of record.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(500);
        sb.append("(columns:");

        if (columnsInOrderMapping != null) {
            boolean sep = false;

            for (Map.Entry<String,Object> entry : columnsInOrderMapping.entrySet()) {
                if (sep) {
                    sb.append(',');
                } else {
                    sep = true;
                }
                sb.append('(');
                sb.append(entry.getKey());
                sb.append(':');
                sb.append(entry.getValue());
                sb.append(')');

                if (sb.length() > 1000) {
                    sb.append("...");
                    break;
                }
            }
        } else {
            sb.append("null");
        }
        sb.append(')');
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyColumns, columnsInOrderMapping);
    }

    /**
     * Compare records for equality.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        Record other = (Record)obj;
        return Objects.equals(keyColumns, other.keyColumns)
            && Objects.equals(columnsInOrderMapping, other.columnsInOrderMapping);
    }
}
