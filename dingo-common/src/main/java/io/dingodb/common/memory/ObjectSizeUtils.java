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

package io.dingodb.common.memory;

import org.openjdk.jol.info.ClassData;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.FieldLayout;
import org.openjdk.jol.layouters.CurrentLayouter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class ObjectSizeUtils {
    public static final long ARRAY_HEADER_SIZE;
    public static final long OBJECT_HEADER_SIZE;
    public static final long OBJECT_PADDING;
    public static final long REFERENCE_SIZE;

    static {
        ARRAY_HEADER_SIZE = ClassLayout.parseClass(int[].class).headerSize();
        OBJECT_HEADER_SIZE = ClassLayout.parseClass(Object.class).headerSize();
        OBJECT_PADDING = 8;
        REFERENCE_SIZE = 8;
    }

    private static ClassLayout parseInstance(Object instance) {
        return new CurrentLayouter().layout(ClassData.parseInstance(instance));
    }


    public static long calculateObjectSize(Object object) {
        ClassLayout layout = parseInstance(object);
        return layout.instanceSize();
    }

    private static long calculateObjectSizeNoPadding(Object object) {
        ClassLayout layout = parseInstance(object);
        FieldLayout lastField = layout.fields().last();
        return lastField.offset() + lastField.size();
    }

    public static final long SIZE_BYTE = calculateObjectSize((byte) 0);
    public static final long SIZE_CHAR = calculateObjectSize((char) 0);
    public static final long SIZE_SHORT = calculateObjectSize((short) 0);
    public static final long SIZE_INTEGER = calculateObjectSize((int) 0);
    public static final long SIZE_LONG = calculateObjectSize((long) 0);
    public static final long SIZE_FLOAT = calculateObjectSize((float) 0.0);
    public static final long SIZE_DOUBLE = calculateObjectSize((double) 0.0);

    public static final long SIZE_DATE = calculateObjectSize(new java.sql.Date(1L));
    public static final long SIZE_TIMESTAMP = calculateObjectSize(new java.sql.Timestamp(1L));
    public static final long SIZE_TIME = calculateObjectSize(new java.sql.Time(1L));

    public static final long SIZE_BOOLEAN = calculateObjectSize(true);

    public static final long BASE_SIZE_BIG_INTEGER = calculateObjectSizeNoPadding(new BigInteger("0"));
    public static final long BASE_SIZE_BIG_DECIMAL = calculateObjectSizeNoPadding(new BigDecimal("0.0"));
    public static final long BASE_SIZE_STRING = calculateObjectSizeNoPadding("");

    public static final long BASE_SIZE_BYTES = calculateObjectSizeNoPadding(new byte[0]);
    public static final long BASE_SIZE_BLOB = calculateObjectSizeNoPadding(new DummyBlob());
    public static final long BASE_SIZE_CLOB = calculateObjectSizeNoPadding(new DummyClob());

    public static final long ARRAY_LIST_BASE_SIZE = calculateObjectSize(new ArrayList<>());
    public static final long HASH_ENTRY_SIZE = calculateObjectSize(new HashMap.SimpleEntry<>(null, null));
    public static final long SIZE_OBJ_REF = REFERENCE_SIZE;

    public static long calculateSize(Object[] tuples) {
        long size = 0;
        size += tuples.length * ObjectSizeUtils.REFERENCE_SIZE;
        for (Object data : tuples) {
            size += ObjectSizeUtils.calculateDataSize(data);
        }
        return size;
    }

    public static long calculateDataSize(Object d) {
        if (d == null) {
            return 0;
        } else if (d instanceof String) {
            String s = (String) d;
            return pad(BASE_SIZE_STRING + s.length() * Character.BYTES);
        } else if (d instanceof Byte) {
            return SIZE_BYTE;
        } else if (d instanceof Short) {
            return SIZE_SHORT;
        } else if (d instanceof Integer) {
            return SIZE_INTEGER;
        } else if (d instanceof Long) {
            return SIZE_LONG;
        } else if (d instanceof BigInteger) {
            BigInteger data = (BigInteger) d;
            return pad(BASE_SIZE_BIG_INTEGER + Math.max(data.bitLength() - 1, 64) / 32 * 4);
        } else if (d instanceof BigDecimal) {
            BigDecimal data = (BigDecimal) d;
            return pad(BASE_SIZE_BIG_DECIMAL + Math.max(data.unscaledValue().bitLength() - 1, 64) / 32 * 4);
        } else if (d instanceof Float) {
            return SIZE_FLOAT;
        } else if (d instanceof Double) {
            return SIZE_DOUBLE;
        } else if (d instanceof java.sql.Date) {
            return SIZE_DATE;
        } else if (d instanceof java.sql.Timestamp) {
            return SIZE_TIMESTAMP;
        } else if (d instanceof java.sql.Time) {
            return SIZE_TIME;
        } else if (d instanceof byte[]) {
            return pad(BASE_SIZE_BYTES + ((byte[]) d).length);
        } else if (d instanceof Blob) {
            long length = 0;
            try {
                length = ((Blob) d).length();
            } catch (SQLException e) {

            }
            return pad(BASE_SIZE_BLOB + length);
        } else if (d instanceof Clob) {
            long length = 0;
            try {
                length = ((Clob) d).length();
            } catch (SQLException e) {

            }
            return pad(BASE_SIZE_CLOB + length * Character.BYTES);
        } else if (d instanceof Boolean) {
            return 0;
        } else {

            return calculateObjectSize(d);
        }
    }

    private static long pad(long size) {
        return (size + OBJECT_PADDING - 1) / OBJECT_PADDING * OBJECT_PADDING;
    }

    private static class DummyBlob {

        private byte[] binaryData = new byte[0];
        private boolean isClosed = false;
    }

    private static class DummyClob {

        private String charData = "";
    }
}
