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

import java.util.List;
import java.util.Map;

/**
 * Column name/value pair.
 */
public final class Column {
    /**
     * The name of the column.
     */
    public final String name;

    /**
     * The value of the column.
     */
    public final Value value;

    /**
     * Constructor, specifying column name and string value.
     * enter a null or empty name.
     *
     * @param name the name of the column
     * @param value the value of the column with string type
     */
    public Column(String name, String value) {
        this.name = name;
        this.value = Value.get(value);
    }

    /**
     * Constructor, specifying column name and byte array value.
     *
     * @param name the name of the column
     * @param value the value of the column with byte array type
     */
    public Column(String name, byte[] value) {
        this.name = name;
        this.value = Value.get(value);
    }

    /**
     * Constructor, specifying column name, byte array value.
     *
     * @param name the name of the column
     * @param value the value of the column with byte array and type
     * @param type column type
     */
    public Column(String name, byte[] value, int type) {
        this.name = name;
        this.value = Value.get(value, type);
    }

    /**
     * Constructor, specifying bin name and byte array segment value.
     *
     * @param name the name of the column
     * @param value the value of the column with byte array and type
     * @param offset byte array segment offset
     * @param length byte array segment length
     */
    public Column(String name, byte[] value, int offset, int length) {
        this.name = name;
        this.value = Value.get(value, offset, length);
    }

    /**
     * Constructor, specifying column name and byte value.
     *
     * @param name the name of the column
     * @param value the value of the column with byte
     */
    public Column(String name, byte value) {
        this.name = name;
        this.value = Value.get(value);
    }

    /**
     * Constructor, specifying column name and integer value.
     * The server will convert all integers to longs.
     *
     * @param name name of the column
     * @param value value of the column
     */
    public Column(String name, int value) {
        this.name = name;
        this.value = Value.get(value);
    }

    /**
     * Constructor, specifying bin name and long value.
     *
     * @param name name of the column
     * @param value value of the column with long type
     */
    public Column(String name, long value) {
        this.name = name;
        this.value = Value.get(value);
    }

    /**
     * Constructor, specifying bin name and double value.
     *
     * @param name name of the column
     * @param value value of the column with double type
     */
    public Column(String name, double value) {
        this.name = name;
        this.value = Value.get(value);
    }

    /**
     * Constructor, specifying bin name and float value.
     *
     * @param name name of the column
     * @param value value of the column with float type
     */
    public Column(String name, float value) {
        this.name = name;
        this.value = Value.get(value);
    }

    /**
     * Constructor, specifying column name and boolean value.
     * a boolean column is sent to the server.
     *
     * @param name name of the column
     * @param value value of the column with boolean type
     */
    public Column(String name, boolean value) {
        this.name = name;
        this.value = Value.get(value);
    }

    /**
     * Create column with a list value.  The list value will be serialized as a server list type.
     *
     * @param name name of the column
     * @param value value of the column with list type
     */
    public Column(String name, List<?> value) {
        this.name = name;
        this.value = Value.get(value);
    }

    /**
     * Create bin with a map value.  The map value will be serialized as a server map type.
     *
     * @param name  name of the column
     * @param value  value of the column with list type
     */
    public Column(String name, Map<?,?> value) {
        this.name = name;
        this.value = Value.get(value);
    }

    /**
     * Create column with a map value and order.  The map value will be serialized as a server map type.
     *
     * @param name  name of the column
     * @param value  value of the column, pass in a {@link java.util.SortedMap} instance if map order is sorted.
     * @param mapOrder  map sorted order.
     */
    public Column(String name, Map<?,?> value, MapOrder mapOrder) {
        this.name = name;
        this.value = Value.get(value, mapOrder);
    }

    /**
     * Create a map column from a list of key/value entries.  The value will be serialized as a
     * server map type with specified mapOrder.
     *
     * @param name  name of the column
     * @param value  list of key/value entries already in desired sorted order
     * @param mapOrder  map sorted order
     */
    public Column(String name, List<? extends Map.Entry<?,?>> value, MapOrder mapOrder) {
        this.name = name;
        this.value = Value.get(value, mapOrder);
    }

    /**
     * Constructor, specifying bin name and value.
     *
     * @param name  name of the column
     * @param value  value is Value type
     */
    public Column(String name, Value value) {
        this.name = name;
        this.value = value;
    }

    /**
     * Constructor, specifying column name and object value.
     * This is the slowest of the Column constructors because the type
     * must be determined using multiple "instanceof" checks.
     *
     * @param name name of the column
     * @param value the value of the column with object type
     */
    public Column(String name, Object value) {
        this.name = name;
        this.value = Value.get(value);
    }

    /**
     * Create column with a blob value.  The value will be java serialized.
     * This method is faster than the Column Object constructor because the blob is converted
     * directly instead of using multiple "instanceof" type checks with a blob default.
     *
     * @param name name of the column
     * @param value value of the column
     */
    public static Column asBlob(String name, Object value) {
        return new Column(name, Value.getAsBlob(value));
    }

    /**
     * Create column with a null value. This is useful for column deletions within a record.
     *
     * @param name name of the column
     */
    public static Column asNull(String name) {
        return new Column(name, Value.getAsNull());
    }

    /**
     * Return string representation of bin.
     */
    @Override
    public String toString() {
        return name + ':' + value;
    }

    /**
     * Compare Bin for equality.
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
        Column other = (Column) obj;
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (value == null) {
            if (other.value != null) {
                return false;
            }
        } else if (!value.equals(other.value)) {
            return false;
        }
        return true;
    }

    /**
     * Return hash code for Bin.
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }
}
