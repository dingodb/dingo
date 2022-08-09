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

package io.dingodb.serial.io;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class BinaryEncoder {
    private byte[] buf;
    private int position = 0;

    public BinaryEncoder(int initialCapacity) {
        this.buf = new byte[initialCapacity];
    }

    public BinaryEncoder(byte[] buf) {
        this.buf = buf;
    }

    public BinaryEncoder(byte[] buf, int position) {
        this.buf = buf;
        this.position = position;
    }

    public boolean readIsNull() {
        return buf[position++] == 0;
    }

    public Short readShort() throws IndexOutOfBoundsException, ClassCastException {
        if (readIsNull()) {
            position += 2;
            return null;
        }
        return (short) (((buf[position++] & 0xFF) << 8)
            | ((buf[position++] & 0xFF) << 0));

    }

    public int readIntNoNull() throws IndexOutOfBoundsException {
        return (((buf[position++] & 0xFF) << 24)
            | ((buf[position++] & 0xFF) << 16)
            | ((buf[position++] & 0xFF) << 8)
            | ((buf[position++] & 0xFF) << 0));
    }

    private void writeNull() throws IndexOutOfBoundsException {
        buf[position++] = 0;
    }

    private void writeNotNull() throws IndexOutOfBoundsException {
        buf[position++] = 1;
    }

    public void writeBoolean(Object bool) throws IndexOutOfBoundsException, ClassCastException {
        if (bool == null) {
            writeNull();
            buf[position++] = 0;
        } else {
            writeNotNull();
            buf[position++] = (byte) ((Boolean) bool ? 1 : 0);
        }
    }

    public void writeShort(Object sh) throws IndexOutOfBoundsException, ClassCastException {
        if (sh == null) {
            writeNull();
            buf[position++] = 0;
            buf[position++] = 0;
        } else {
            writeNotNull();
            buf[position++] = (byte) ((Short) sh >>> 8);
            buf[position++] = (byte) ((Short) sh >>> 0);
        }
    }

    public void writeInt(Object in) throws IndexOutOfBoundsException, ClassCastException {
        if (in == null) {
            writeNull();
            buf[position++] = 0;
            buf[position++] = 0;
            buf[position++] = 0;
            buf[position++] = 0;
        } else {
            writeNotNull();
            buf[position++] = (byte) ((Integer) in >>> 24);
            buf[position++] = (byte) ((Integer) in >>> 16);
            buf[position++] = (byte) ((Integer) in >>> 8);
            buf[position++] = (byte) ((Integer) in >>> 0);
        }
    }

    public void writeLong(Object ln) throws IndexOutOfBoundsException, ClassCastException {
        if (ln == null) {
            writeNull();
            buf[position++] = 0;
            buf[position++] = 0;
            buf[position++] = 0;
            buf[position++] = 0;
            buf[position++] = 0;
            buf[position++] = 0;
            buf[position++] = 0;
            buf[position++] = 0;
        } else {
            writeNotNull();
            buf[position++] = (byte) ((Long) ln >>> 56);
            buf[position++] = (byte) ((Long) ln >>> 48);
            buf[position++] = (byte) ((Long) ln >>> 40);
            buf[position++] = (byte) ((Long) ln >>> 32);
            buf[position++] = (byte) ((Long) ln >>> 24);
            buf[position++] = (byte) ((Long) ln >>> 16);
            buf[position++] = (byte) ((Long) ln >>> 8);
            buf[position++] = (byte) ((Long) ln >>> 0);
        }
    }

    public void writeFloat(Object fo) throws IndexOutOfBoundsException, ClassCastException {
        if (fo == null) {
            writeInt(null);
        } else {
            writeInt(Float.floatToIntBits((Float) fo));
        }
    }

    public void writeDouble(Object dl) throws IndexOutOfBoundsException, ClassCastException {
        if (dl == null) {
            writeLong(null);
        } else {
            writeLong(Double.doubleToLongBits((Double) dl));
        }
    }

    public void writeString(Object string) throws IndexOutOfBoundsException, ClassCastException {
        writeString(string, 0);
    }

    public void writeString(Object string, int maxLength) throws IndexOutOfBoundsException, ClassCastException {
        if (string == null) {
            ensureRemainder(1);
            writeNull();
        } else if (((String) string).length() == 0) {
            ensureRemainder(5);
            writeInt(0);
        } else {
            byte[] value = ((String) string).getBytes(StandardCharsets.UTF_8);
            int length = maxLength == 0 ? value.length : maxLength - 5;
            ensureRemainder(5 + length);
            writeInt(length);
            System.arraycopy(value, 0, buf, position, length);
            position += length;
        }
    }

    public void updateString(Object string) throws IndexOutOfBoundsException, ClassCastException {
        updateString(string, 0);
    }

    public void updateString(Object string, int maxLength) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = position;
        skipString();
        int remainBytesLength = buf.length - position;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, position, buf.length);
        }
        position = startMark;
        writeString(string, maxLength);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, position, remainBytesLength);
        }
    }

    public void writeBytes(Object bytes) throws IndexOutOfBoundsException, ClassCastException {
        writeBytes(bytes, 0);
    }

    public void writeBytes(Object bytes, int maxLength) throws IndexOutOfBoundsException, ClassCastException {
        if (bytes == null) {
            ensureRemainder(1);
            writeNull();
        } else if (((byte[]) bytes).length == 0) {
            ensureRemainder(5);
            writeInt(0);
        } else {
            byte[] value = (byte[]) bytes;
            int length = maxLength == 0 ? value.length : maxLength - 5;
            ensureRemainder(5 + length);
            writeInt(length);
            System.arraycopy(value, 0, buf, position, length);
            position += length;
        }
    }

    public void updateBytes(Object bytes) throws IndexOutOfBoundsException, ClassCastException {
        updateBytes(bytes, 0);
    }

    public void updateBytes(Object bytes, int maxLength) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = position;
        skipString();
        int remainBytesLength = buf.length - position;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, position, buf.length);
        }
        position = startMark;
        writeBytes(bytes, maxLength);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, position, remainBytesLength);
        }
    }

    public void skipString() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(readIntNoNull());
        }
    }

    public void writeBooleanList(Object booleanList) throws IndexOutOfBoundsException, ClassCastException {
        if (booleanList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Boolean> list = (List<Boolean>) booleanList;
            ensureRemainder(5 + (2 * list.size()));
            writeInt(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeBoolean(list.get(i));
            }
        }
    }

    public void updateBooleanList(Object booleanList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = position;
        skipBooleanList();
        int remainBytesLength = buf.length - position;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, position, buf.length);
        }
        position = startMark;
        writeBooleanList(booleanList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, position, remainBytesLength);
        }
    }

    public void skipBooleanList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(2 * readIntNoNull());
        }
    }

    public void writeShortList(Object shortList) throws IndexOutOfBoundsException, ClassCastException {
        if (shortList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Short> list = (List<Short>) shortList;
            ensureRemainder(5 + (3 * list.size()));
            writeInt(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeShort(list.get(i));
            }
        }
    }

    public void updateShortList(Object shortList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = position;
        skipShortList();
        int remainBytesLength = buf.length - position;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, position, buf.length);
        }
        position = startMark;
        writeShortList(shortList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, position, remainBytesLength);
        }
    }

    public void skipShortList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(3 * readIntNoNull());
        }
    }

    public void writeIntegerList(Object integerList) throws IndexOutOfBoundsException, ClassCastException {
        if (integerList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Integer> list = (List<Integer>) integerList;
            ensureRemainder(5 + (5 * list.size()));
            writeInt(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeInt(list.get(i));
            }
        }
    }

    public void updateIntegerList(Object integerList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = position;
        skipIntegerList();
        int remainBytesLength = buf.length - position;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, position, buf.length);
        }
        position = startMark;
        writeIntegerList(integerList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, position, remainBytesLength);
        }
    }

    public void skipIntegerList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(5 * readIntNoNull());
        }
    }

    public void writeFloatList(Object floatList) throws IndexOutOfBoundsException, ClassCastException {
        if (floatList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Float> list = (List<Float>) floatList;
            ensureRemainder(5 + (5 * list.size()));
            writeInt(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeFloat(list.get(i));
            }
        }
    }

    public void updateFloatList(Object floatList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = position;
        skipFloatList();
        int remainBytesLength = buf.length - position;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, position, buf.length);
        }
        position = startMark;
        writeFloatList(floatList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, position, remainBytesLength);
        }
    }

    public void skipFloatList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(5 * readIntNoNull());
        }
    }

    public void writeLongList(Object longList) throws IndexOutOfBoundsException, ClassCastException {
        if (longList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Long> list = (List<Long>) longList;
            ensureRemainder(5 + (9 * list.size()));
            writeInt(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeLong(list.get(i));
            }
        }
    }

    public void updateLongList(Object longList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = position;
        skipLongList();
        int remainBytesLength = buf.length - position;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, position, buf.length);
        }
        position = startMark;
        writeLongList(longList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, position, remainBytesLength);
        }
    }

    public void skipLongList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(9 * readIntNoNull());
        }
    }

    public void writeDoubleList(Object doubleList) throws IndexOutOfBoundsException, ClassCastException {
        if (doubleList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Double> list = (List<Double>) doubleList;
            ensureRemainder(5 + (9 * list.size()));
            writeInt(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeDouble(list.get(i));
            }
        }
    }

    public void updateDoubleList(Object doubleList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = position;
        skipDoubleList();
        int remainBytesLength = buf.length - position;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, position, buf.length);
        }
        position = startMark;
        writeDoubleList(doubleList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, position, remainBytesLength);
        }
    }

    public void skipDoubleList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(9 * readIntNoNull());
        }
    }

    public void writeStringList(Object stringList) throws IndexOutOfBoundsException, ClassCastException {
        if (stringList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<String> list = (List<String>) stringList;
            ensureRemainder(5);
            writeInt(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeString(list.get(i));
            }
        }
    }

    public void updateStringList(Object stringList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = position;
        skipStringList();
        int remainBytesLength = buf.length - position;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, position, buf.length);
        }
        position = startMark;
        writeStringList(stringList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, position, remainBytesLength);
        }
    }

    public void skipStringList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            int size = readIntNoNull();
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    skipString();
                }
            }
        }
    }

    public void skip(int nu) {
        this.position += nu;
    }

    private void ensureRemainder(int length) {
        if (buf.length - position < length) {
            buf = Arrays.copyOf(buf, position + length);
        }
    }

    public byte[] getByteArray() {
        if (position != buf.length) {
            buf = Arrays.copyOf(buf, position);
        }
        return buf;
    }
}
