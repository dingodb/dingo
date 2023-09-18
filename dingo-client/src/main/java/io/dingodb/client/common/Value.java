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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Polymorphic value classes used to efficiently serialize objects into the wire protocol.
 */
public abstract class Value {
    /**
     * Should client send boolean particle type for a boolean bin.  If false,
     * an integer particle type (1 or 0) is sent instead. Must be false for server
     * versions less than 5.6 which do not support boolean bins. Can set to true for
     * server 5.6+.
     */
    public static boolean UseBoolBin = false;

    /**
     * Null value.
     */
    public static final Value NULL = NullValue.INSTANCE;

    /**
     * Infinity value to be used in CDT range comparisons only.
     */
    public static final Value INFINITY = new InfinityValue();


    /**
     * Get string or null value instance.
     * @param value value
     * @return value
     */
    public static Value get(String value) {
        return (value == null) ? NullValue.INSTANCE : new StringValue(value);
    }

    /**
     * Get byte array or null value instance.
     * @param value value
     * @return value
     */
    public static Value get(byte[] value) {
        return (value == null) ? NullValue.INSTANCE : new BytesValue(value);
    }

    /**
     * Get byte array with type or null value instance.
     * @param value value
     * @param type type
     * @return value
     */
    public static Value get(byte[] value, int type) {
        return (value == null) ? NullValue.INSTANCE : new BytesValue(value, type);
    }

    /**
     * Get byte segment or null value instance.
     * @param value value
     * @param offset offset
     * @param length length
     * @return value
     */
    public static Value get(byte[] value, int offset, int length) {
        return (value == null) ? NullValue.INSTANCE : new ByteSegmentValue(value, offset, length);
    }

    /**
     * Get byte segment or null value instance.
     * @param bb byte buffer
     * @return value
     */
    public static Value get(ByteBuffer bb) {
        return (bb == null) ? NullValue.INSTANCE : new BytesValue(bb.array());
    }

    /**
     * Get byte value instance.
     * @param value value
     * @return value
     */
    public static Value get(byte value) {
        return new ByteValue(value);
    }

    /**
     * Get integer value instance.
     * @param value value
     * @return value
     */
    public static Value get(int value) {
        return new IntegerValue(value);
    }

    /**
     * Get long value instance.
     * @param value value
     * @return value
     */
    public static Value get(long value) {
        return new LongValue(value);
    }

    /**
     * Get double value instance.
     * @param value value
     * @return value
     */
    public static Value get(double value) {
        return new DoubleValue(value);
    }

    /**
     * Get float value instance.
     * @param value value
     * @return value
     */
    public static Value get(float value) {
        return new FloatValue(value);
    }

    /**
     * Get boolean value instance.
     * @param value value
     * @return value
     */
    public static Value get(boolean value) {
        if (UseBoolBin) {
            return new BooleanValue(value);
        } else {
            return new BoolIntValue(value);
        }
    }

    /**
     * Get enum value string instance.
     * @param value value
     * @return value
     */
    public static Value get(Enum<?> value) {
        return (value == null) ? NullValue.INSTANCE : new StringValue(value.toString());
    }

    /**
     * Get UUID value string instance.
     * @param value value
     * @return value
     */
    public static Value get(UUID value) {
        return (value == null) ? NullValue.INSTANCE : new StringValue(value.toString());
    }

    /**
     * Get list or null value instance.
     * @param value value
     * @return value
     */
    public static Value get(List<?> value) {
        return (value == null) ? NullValue.INSTANCE : new ListValue(value);
    }

    /**
     * Get map or null value instance.
     * @param value value
     * @return value
     */
    public static Value get(Map<?,?> value) {
        return (value == null) ? NullValue.INSTANCE : new MapValue(value);
    }

    /**
     * Get value array instance.
     * @param value value
     * @return value
     */
    public static Value get(Value[] value) {
        return (value == null) ? NullValue.INSTANCE : new ValueArray(value);
    }

    /**
     * Determine value given generic object.
     * This is the slowest of the Value get() methods.
     * Useful when copying records from one cluster to another.
     * @param value value
     * @return value
     */
    public static Value get(Object value) {
        if (value == null) {
            return NullValue.INSTANCE;
        }

        if (value instanceof Value) {
            return (Value)value;
        }

        if (value instanceof byte[]) {
            return new BytesValue((byte[])value);
        }

        if (value instanceof String) {
            return new StringValue((String)value);
        }

        if (value instanceof Integer) {
            return new IntegerValue((Integer)value);
        }

        if (value instanceof Long) {
            return new LongValue((Long)value);
        }

        if (value instanceof Double) {
            return new DoubleValue((Double)value);
        }

        if (value instanceof Float) {
            return new FloatValue((Float)value);
        }

        if (value instanceof Boolean) {
            if (UseBoolBin) {
                return new BooleanValue((Boolean)value);
            } else {
                return new BoolIntValue((Boolean)value);
            }
        }

        if (value instanceof Byte) {
            return new ByteValue((byte)value);
        }

        if (value instanceof Enum) {
            return new StringValue(value.toString());
        }

        if (value instanceof UUID) {
            return new StringValue(value.toString());
        }

        if (value instanceof List<?>) {
            return new ListValue((List<?>)value);
        }

        if (value instanceof Map<?,?>) {
            return new MapValue((Map<?,?>)value);
        }

        if (value instanceof ByteBuffer) {
            ByteBuffer bb = (ByteBuffer)value;
            return new BytesValue(bb.array());
        }

        return new BlobValue(value);
    }

    /**
     * Get blob or null value instance.
     * @param value value
     * @return value
     */
    public static Value getAsBlob(Object value) {
        return (value == null) ? NullValue.INSTANCE : new BlobValue(value);
    }

    /**
     * Get null value instance.
     * @return value
     */
    public static Value getAsNull() {
        return NullValue.INSTANCE;
    }

    /**
     * Get value from Record object. Useful when copying records from one cluster to another.
     * @param value value
     * @return value
     */
    @Deprecated
    public static Value getFromRecordObject(Object value) {
        return Value.get(value);
    }


    /**
     * Validate if value type can be used as a key.
     */
    public void validateKeyType() {
    }

    /**
     * Get wire protocol value type.
     * @return type
     */
    public abstract int getType();

    /**
     * Return original value as an Object.
     * @return object
     */
    public abstract Object getObject();


    public Number value() {
        return null;
    }

    public final class ParticleType {
        public static final int NULL = 0;
        public static final int INTEGER = 1;
        public static final int DOUBLE = 2;
        public static final int STRING = 3;
        public static final int BLOB = 4;
        public static final int JBLOB = 7;
        public static final int BOOL = 17;
        public static final int MAP = 19;
        public static final int LIST = 20;
    }

    /**
     * Empty value.
     */
    public static final class NullValue extends Value {
        public static final NullValue INSTANCE = new NullValue();

        @Override
        public void validateKeyType() {
            // throw new DingoClientException(ResultCode.PARAMETER_ERROR, "Invalid key type: null");
        }

        @Override
        public int getType() {
            return ParticleType.NULL;
        }

        @Override
        public Object getObject() {
            return null;
        }

        @Override
        public String toString() {
            return null;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null) {
                return true;
            }
            return this.getClass().equals(other.getClass());
        }

        @Override
        public final int hashCode() {
            return 0;
        }
    }

    public static String bytesToHexString(byte[] buf) {
        if (buf == null || buf.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(buf.length * 2);

        for (int i = 0; i < buf.length; i++) {
            sb.append(String.format("%02x", buf[i]));
        }
        return sb.toString();
    }

    public static String bytesToHexString(byte[] buf, int offset, int length) {
        StringBuilder sb = new StringBuilder(length * 2);

        for (int i = offset; i < length; i++) {
            sb.append(String.format("%02x", buf[i]));
        }
        return sb.toString();
    }

    /**
     * Byte array value.
     */
    public static final class BytesValue extends Value {
        private final byte[] bytes;
        private final int type;

        public BytesValue(byte[] bytes) {
            this.bytes = bytes;
            this.type = ParticleType.BLOB;
        }

        public BytesValue(byte[] bytes, int type) {
            this.bytes = bytes;
            this.type = type;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public Object getObject() {
            return bytes;
        }

        @Override
        public String toString() {
            return bytesToHexString(bytes);
        }

        @Override
        public boolean equals(Object other) {
            return (other != null
                && this.getClass().equals(other.getClass())
                && Arrays.equals(this.bytes, ((BytesValue)other).bytes));
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    /**
     * Byte segment value.
     */
    public static final class ByteSegmentValue extends Value {
        private final byte[] bytes;
        private final int offset;
        private final int length;

        public ByteSegmentValue(byte[] bytes, int offset, int length) {
            this.bytes = bytes;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public int getType() {
            return ParticleType.BLOB;
        }

        @Override
        public Object getObject() {
            return this;
        }

        @Override
        public String toString() {
            return bytesToHexString(bytes, offset, length);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }

            if (! this.getClass().equals(obj.getClass())) {
                return false;
            }
            ByteSegmentValue other = (ByteSegmentValue)obj;

            if (this.length != other.length) {
                return false;
            }

            for (int i = 0; i < length; i++) {
                if (this.bytes[this.offset + i] != other.bytes[other.offset + i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = 1;
            for (int i = 0; i < length; i++) {
                result = 31 * result + bytes[offset + i];
            }
            return result;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public int getOffset() {
            return offset;
        }

        public int getLength() {
            return length;
        }
    }

    /**
     * Byte value.
     */
    public static final class ByteValue extends Value {
        private final byte value;

        public ByteValue(byte value) {
            this.value = value;
        }

        @Override
        public int getType() {
            // The server does not natively handle one byte, so store as long (8 byte integer).
            return ParticleType.INTEGER;
        }

        @Override
        public Object getObject() {
            return value;
        }

        @Override
        public String toString() {
            return Byte.toString(value);
        }

        @Override
        public boolean equals(Object other) {
            return (other != null
                && this.getClass().equals(other.getClass())
                && this.value == ((ByteValue)other).value);
        }

        @Override
        public int hashCode() {
            return value;
        }

        @Override
        public Number value() {
            return value;
        }
    }

    /**
     * String value.
     */
    public static final class StringValue extends Value {
        private final String value;

        public StringValue(String value) {
            this.value = value;
        }

        @Override
        public int getType() {
            return ParticleType.STRING;
        }

        @Override
        public Object getObject() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public boolean equals(Object other) {
            return (other != null
                && this.getClass().equals(other.getClass())
                && this.value.equals(((StringValue)other).value));
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    /**
     * Integer value.
     */
    public static final class IntegerValue extends Value {
        private final int value;

        public IntegerValue(int value) {
            this.value = value;
        }

        @Override
        public int getType() {
            return ParticleType.INTEGER;
        }

        @Override
        public Object getObject() {
            return value;
        }

        @Override
        public String toString() {
            return Integer.toString(value);
        }

        @Override
        public boolean equals(Object other) {
            return (other != null
                && this.getClass().equals(other.getClass())
                && this.value == ((IntegerValue)other).value);
        }

        @Override
        public int hashCode() {
            return value;
        }

        @Override
        public Number value() {
            return value;
        }
    }

    /**
     * Long value.
     */
    public static final class LongValue extends Value {
        private final long value;

        public LongValue(long value) {
            this.value = value;
        }

        @Override
        public int getType() {
            return ParticleType.INTEGER;
        }

        @Override
        public Object getObject() {
            return value;
        }

        @Override
        public String toString() {
            return Long.toString(value);
        }

        @Override
        public boolean equals(Object other) {
            return (other != null
                && this.getClass().equals(other.getClass())
                && this.value == ((LongValue)other).value);
        }

        @Override
        public int hashCode() {
            return (int)(value ^ (value >>> 32));
        }

        @Override
        public Number value() {
            return value;
        }
    }

    /**
     * Double value.
     */
    public static final class DoubleValue extends Value {
        private final double value;

        public DoubleValue(double value) {
            this.value = value;
        }

        @Override
        public int getType() {
            return ParticleType.DOUBLE;
        }

        @Override
        public Object getObject() {
            return value;
        }

        @Override
        public String toString() {
            return Double.toString(value);
        }

        @Override
        public boolean equals(Object other) {
            return (other != null
                && this.getClass().equals(other.getClass())
                && this.value == ((DoubleValue)other).value);
        }

        @Override
        public int hashCode() {
            long bits = Double.doubleToLongBits(value);
            return (int)(bits ^ (bits >>> 32));
        }

        @Override
        public Number value() {
            return value;
        }
    }

    /**
     * Float value.
     */
    public static final class FloatValue extends Value {
        private final float value;

        public FloatValue(float value) {
            this.value = value;
        }

        @Override
        public int getType() {
            return ParticleType.DOUBLE;
        }

        @Override
        public Object getObject() {
            return value;
        }

        @Override
        public String toString() {
            return Float.toString(value);
        }

        @Override
        public boolean equals(Object other) {
            return (other != null
                && this.getClass().equals(other.getClass())
                && this.value == ((FloatValue)other).value);
        }

        @Override
        public int hashCode() {
            return Float.floatToIntBits(value);
        }

        @Override
        public Number value() {
            return value;
        }
    }

    /**
     * Boolean value.
     */
    public static final class BooleanValue extends Value {
        private final boolean value;

        public BooleanValue(boolean value) {
            this.value = value;
        }

        @Override
        public void validateKeyType() {
            // throw new DingoClientException(ResultCode.PARAMETER_ERROR, "Invalid key type: boolean");
        }

        @Override
        public int getType() {
            return ParticleType.BOOL;
        }

        @Override
        public Object getObject() {
            return value;
        }

        @Override
        public String toString() {
            return Boolean.toString(value);
        }

        @Override
        public boolean equals(Object other) {
            return (other != null
                && this.getClass().equals(other.getClass())
                && this.value == ((BooleanValue)other).value);
        }

        @Override
        public int hashCode() {
            return value ? 1231 : 1237;
        }

        @Override
        public Number value() {
            return value ? 1 : 0;
        }
    }

    /**
     * Boolean value that converts to integer when sending a bin to the server.
     * This class will be deleted once full conversion to boolean particle type
     * is complete.
     */
    public static final class BoolIntValue extends Value {
        private final boolean value;

        public BoolIntValue(boolean value) {
            this.value = value;
        }

        @Override
        public void validateKeyType() {
            // throw new DingoClientException(ResultCode.PARAMETER_ERROR, "Invalid key type: BoolIntValue");
        }

        @Override
        public int getType() {
            // The server does not natively handle boolean, so store as long (8 byte integer).
            return ParticleType.INTEGER;
        }

        @Override
        public Object getObject() {
            return value;
        }

        @Override
        public String toString() {
            return Boolean.toString(value);
        }

        @Override
        public boolean equals(Object other) {
            return (other != null
                && this.getClass().equals(other.getClass())
                && this.value == ((BoolIntValue)other).value);
        }

        @Override
        public int hashCode() {
            return value ? 1231 : 1237;
        }

        @Override
        public Number value() {
            return value ? 1L : 0L;
        }
    }

    /**
     * Blob value.
     */
    public static final class BlobValue extends Value {
        private final Object object;
        private byte[] bytes;

        public BlobValue(Object object) {
            this.object = object;
        }

        @Override
        public void validateKeyType() {
            // throw new DingoClientException(ResultCode.PARAMETER_ERROR, "Invalid key type: jblob");
        }

        @Override
        public int getType() {
            return ParticleType.JBLOB;
        }

        @Override
        public Object getObject() {
            return object;
        }


        @Override
        public String toString() {
            return bytesToHexString(bytes);
        }

        @Override
        public boolean equals(Object other) {
            return (other != null
                && this.getClass().equals(other.getClass())
                && this.object.equals(((BlobValue)other).object));
        }

        @Override
        public int hashCode() {
            return object.hashCode();
        }
    }

    /**
     * Value array.
     */
    public static final class ValueArray extends Value {
        private final Value[] array;
        private byte[] bytes;

        public ValueArray(Value[] array) {
            this.array = array;
        }

        @Override
        public void validateKeyType() {
            // throw new DingoClientException(ResultCode.PARAMETER_ERROR, "Invalid key type: value[]");
        }

        @Override
        public int getType() {
            return ParticleType.LIST;
        }

        @Override
        public Object getObject() {
            return array;
        }

        @Override
        public String toString() {
            return Arrays.toString(array);
        }

        @Override
        public boolean equals(Object other) {
            return (other != null
                && this.getClass().equals(other.getClass())
                && Arrays.equals(this.array, ((ValueArray)other).array));
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(array);
        }
    }

    /**
     * List value.
     */
    public static final class ListValue extends Value {
        private final List<?> list;
        private byte[] bytes;

        public ListValue(List<?> list) {
            this.list = list;
        }

        @Override
        public void validateKeyType() {
            // throw new DingoClientException(ResultCode.PARAMETER_ERROR, "Invalid key type: list");
        }

        @Override
        public int getType() {
            return ParticleType.LIST;
        }

        @Override
        public Object getObject() {
            return list;
        }

        @Override
        public String toString() {
            return list.toString();
        }

        @Override
        public boolean equals(Object other) {
            return (other != null
                && this.getClass().equals(other.getClass())
                && this.list.equals(((ListValue)other).list));
        }

        @Override
        public int hashCode() {
            return list.hashCode();
        }
    }

    /**
     * Map value.
     */
    public static final class MapValue extends Value {
        private final Map<?,?> map;
        private byte[] bytes;

        public MapValue(Map<?,?> map)  {
            this.map = map;
        }

        @Override
        public void validateKeyType() {
            // throw new DingoClientException(ResultCode.PARAMETER_ERROR, "Invalid key type: map");
        }

        @Override
        public int getType() {
            return ParticleType.MAP;
        }

        @Override
        public Object getObject() {
            return map;
        }

        @Override
        public String toString() {
            return map.toString();
        }

        @Override
        public boolean equals(Object other) {
            return (other != null
                && this.getClass().equals(other.getClass())
                && this.map.equals(((MapValue)other).map));
        }

        @Override
        public int hashCode() {
            return map.hashCode();
        }

    }

    /**
     * Sorted map value.
     */
    public static final class SortedMapValue extends Value {
        private final List<? extends Map.Entry<?,?>> list;
        private byte[] bytes;

        public SortedMapValue(List<? extends Map.Entry<?,?>> list)  {
            this.list = list;
        }

        @Override
        public void validateKeyType() {
            // throw new DingoClientException(ResultCode.PARAMETER_ERROR, "Invalid key type: map");
        }

        @Override
        public int getType() {
            return ParticleType.MAP;
        }

        @Override
        public Object getObject() {
            return list;
        }


        @Override
        public String toString() {
            return list.toString();
        }

        @Override
        public boolean equals(Object other) {
            if (other == null || ! this.getClass().equals(other.getClass())) {
                return false;
            }
            SortedMapValue o = (SortedMapValue)other;
            return this.list.equals(o.list);
        }

        @Override
        public int hashCode() {
            return list.hashCode();
        }
    }

    /**
     * Infinity value.
     */
    public static final class InfinityValue extends Value {
        @Override
        public void validateKeyType() {
            //throw new DingoClientException(ResultCode.PARAMETER_ERROR, "Invalid key type: INF");
        }

        @Override
        public int getType() {
            // throw new DingoClientException(ResultCode.PARAMETER_ERROR, "Invalid particle type: INF");
            return 0;
        }

        @Override
        public Object getObject() {
            return null;
        }

        @Override
        public String toString() {
            return "INF";
        }

        @Override
        public boolean equals(Object other) {
            return (other != null && this.getClass().equals(other.getClass()));
        }

        @Override
        public final int hashCode() {
            return 0;
        }
    }
}
