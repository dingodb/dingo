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

import io.dingodb.sdk.annotation.DingoColumn;

import java.lang.reflect.Field;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Container object for records.  Records are equivalent to rows.
 */
public final class Record {
    /**
     * Map of requested name/value columns.
     */
    private final Map<String, Object> columns;


    /**
     * Constructor Record by TableDefinition and columns.
     * @param columnsInTable columns defined in table definition(sequential as create table).
     * @param inputColumns   real record columns(the column order is not important)
     */
    public Record(final List<String> columnsInTable, final Column[] inputColumns) {
        columns = new HashMap<String, Object>();
        for (String columnName: columnsInTable) {
            for (Column column: inputColumns) {
                if (Objects.equals(column.name, columnName)) {
                    columns.put(columnName, column.value);
                }
            }
        }
    }

    private Record(final Map<String, Object> columns) {
        this.columns = columns;
    }

    /**
     * Constructor a record from a object instance.
     * @param instance input object value
     */
    public Record(Object instance) {
        columns = new HashMap<String, Object>();
        for (Field field: instance.getClass().getDeclaredFields()) {
            String fieldName = field.getName();
            if (field.isAnnotationPresent(DingoColumn.class)) {
                String localFieldName = field.getAnnotation(DingoColumn.class).name();
                boolean isOK = localFieldName != null && !localFieldName.isEmpty();
                if (isOK) {
                    fieldName = localFieldName;
                }
            }
            field.setAccessible(true);
            try {
                Object value = field.get(instance);
                columns.put(fieldName, value);
                /*
                for (Annotation annotation: field.getAnnotations()) {
                    if (annotation instanceof DingoEmbed) {
                        if (value instanceof List) {
                            List<Object> list = (List<Object>) value;
                            List<Object> result = new ArrayList<>();
                            for (Object item: list) {
                                result.add(item);
                            }
                            columns.put(fieldName, Arrays.asList(result));
                        } else {
                            columns.put(fieldName, value);
                        }
                    }
                }
                */
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Record toDingoRecord(Record record) {
        Map<String, Object> columns = new HashMap<String, Object>();
        for (Map.Entry<String, Object> entry: record.columns.entrySet()) {
            Object value = entry.getValue();
            if (entry.getValue() instanceof Value) {
                value = ((Value) entry.getValue()).getObject();
            }
            if (value instanceof Date) {
                value = ((Date) value).getTime();
            } else if (value instanceof Time) {
                value = ((Time) value).getTime();
            } else if (value instanceof Timestamp) {
                value = ((Timestamp) value).getTime();
            }
            columns.put(entry.getKey(), value);
        }
        return new Record(columns);
    }


    /**
     * Get bin value given bin name.
     * Enter empty string ("") for servers configured as single-bin.
     */
    public Object getValue(String name) {
        return (columns == null) ? null : columns.get(name);
    }

    /**
     * Get bin value as String.
     */
    public String getString(String name) {
        return (String)getValue(name);
    }

    /**
     * Get bin value as double.
     */
    public double getDouble(String name) {
        // The server may return number as double or long.
        // Convert bits if returned as long.
        Object result = getValue(name);
        return (result instanceof Double)
            ? (Double)result : (result != null) ? Double.longBitsToDouble((Long)result) : 0.0;
    }

    /**
     * Get bin value as float.
     */
    public float getFloat(String name) {
        return (float)getDouble(name);
    }

    /**
     * Get bin value as long.
     */
    public long getLong(String name) {
        // The server always returns numbers as longs if column found.
        // If column not found, the result will be null.  Convert null to zero.
        Object result = getValue(name);
        return (result != null) ? (Long)result : 0;
    }

    /**
     * Get bin value as int.
     */
    public int getInt(String name) {
        // The server always returns numbers as longs, so get long and cast.
        return (int)getLong(name);
    }

    /**
     * Get bin value as short.
     */
    public short getShort(String name) {
        // The server always returns numbers as longs, so get long and cast.
        return (short)getLong(name);
    }

    /**
     * Get bin value as byte.
     */
    public byte getByte(String name) {
        // The server always returns numbers as longs, so get long and cast.
        return (byte)getLong(name);
    }

    /**
     * Get bin value as boolean.
     */
    public boolean getBoolean(String name) {
        // The server may return boolean as boolean or long (created by older clients).
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
     * Get bin value as list.
     */
    public List<?> getList(String name) {
        return (List<?>)getValue(name);
    }

    /**
     * Get bin value as map.
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

        if (columns != null) {
            boolean sep = false;

            for (Map.Entry<String,Object> entry : columns.entrySet()) {
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
        return Objects.hash(columns);
    }

    /**
     * Compare records for equality.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Record other = (Record) obj;
        if (columns == null) {
            return other.columns == null;
        } else {
            return columns.equals(other.columns);
        }
    }
}
