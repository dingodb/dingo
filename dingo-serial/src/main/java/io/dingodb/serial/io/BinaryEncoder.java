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
    private byte[] lengthBuf;
    private int forwardPosition = 0;
    private int reversePosition = 0;

    public BinaryEncoder(int initialCapacity) {
        this.buf = new byte[initialCapacity];
    }

    public BinaryEncoder(int forwardInitCap, int reverseInitCap) {
        this.buf = new byte[forwardInitCap];
        this.lengthBuf = new byte[reverseInitCap];
        this.reversePosition = reverseInitCap - 1;
    }

    public BinaryEncoder(byte[] buf) {
        this.buf = buf;
    }

    public BinaryEncoder(byte[] buf, int reverseCap) {
        int forwardCap = buf.length - reverseCap;
        this.buf = new byte[forwardCap];
        this.lengthBuf = new byte[reverseCap];
        System.arraycopy(buf, 0, this.buf, 0, forwardCap);
        System.arraycopy(buf, forwardCap, this.lengthBuf, 0, reverseCap);
        this.reversePosition = reverseCap - 1;
    }

    public BinaryEncoder(byte[] forwardBuf, byte[] reverseBuf) {
        this.buf = forwardBuf;
        this.lengthBuf = reverseBuf;
        this.reversePosition = lengthBuf.length - 1;
    }

    public void write(byte b) {
        ensureRemainder(1);
        buf[forwardPosition++] = b;
    }

    public void writeBoolean(Object bool) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(2);
        if (bool == null) {
            writeNull();
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            buf[forwardPosition++] = (byte) ((Boolean) bool ? 1 : 0);
        }
    }

    public void writeShort(Object sh) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(3);
        if (sh == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            buf[forwardPosition++] = (byte) ((Short) sh >>> 8);
            buf[forwardPosition++] = (byte) ((Short) sh >>> 0);
        }
    }

    public void writeKeyShort(Object sh) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(3);
        if (sh == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            buf[forwardPosition++] = (byte) ((Short) sh >>> 8 ^ 0x80);
            buf[forwardPosition++] = (byte) ((Short) sh >>> 0);
        }
    }

    public void writeInt(Object in) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(5);
        if (in == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            buf[forwardPosition++] = (byte) ((Integer) in >>> 24);
            buf[forwardPosition++] = (byte) ((Integer) in >>> 16);
            buf[forwardPosition++] = (byte) ((Integer) in >>> 8);
            buf[forwardPosition++] = (byte) ((Integer) in >>> 0);
        }
    }

    public void writeKeyInt(Object in) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(5);
        if (in == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            buf[forwardPosition++] = (byte) ((Integer) in >>> 24 ^ 0x80);
            buf[forwardPosition++] = (byte) ((Integer) in >>> 16);
            buf[forwardPosition++] = (byte) ((Integer) in >>> 8);
            buf[forwardPosition++] = (byte) ((Integer) in >>> 0);
        }
    }

    public void writeFloat(Object fo) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(5);
        if (fo == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            int in = Float.floatToIntBits((Float) fo);
            buf[forwardPosition++] = (byte) (in >>> 24);
            buf[forwardPosition++] = (byte) (in >>> 16);
            buf[forwardPosition++] = (byte) (in >>> 8);
            buf[forwardPosition++] = (byte) in;
        }
    }

    public void writeKeyFloat(Object fo) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(5);
        if (fo == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            int in = (Float.floatToIntBits((Float) fo));
            if (in >= 0) {
                buf[forwardPosition++] = (byte) (in >>> 24 ^ 0x80);
                buf[forwardPosition++] = (byte) (in >>> 16);
                buf[forwardPosition++] = (byte) (in >>> 8);
                buf[forwardPosition++] = (byte) in;
            } else {
                buf[forwardPosition++] = (byte) (~ in >>> 24);
                buf[forwardPosition++] = (byte) (~ in >>> 16);
                buf[forwardPosition++] = (byte) (~ in >>> 8);
                buf[forwardPosition++] = (byte) ~ in;
            }
        }
    }

    public void writeLong(Object ln) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(9);
        if (ln == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            buf[forwardPosition++] = (byte) ((Long) ln >>> 56);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 48);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 40);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 32);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 24);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 16);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 8);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 0);
        }
    }

    public void writeKeyLong(Object ln) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(9);
        if (ln == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            buf[forwardPosition++] = (byte) ((Long) ln >>> 56 ^ 0x80);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 48);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 40);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 32);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 24);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 16);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 8);
            buf[forwardPosition++] = (byte) ((Long) ln >>> 0);
        }
    }

    public void writeDouble(Object dl) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(9);
        if (dl == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            long ln = Double.doubleToLongBits((Double) dl);
            buf[forwardPosition++] = (byte) (ln >>> 56);
            buf[forwardPosition++] = (byte) (ln >>> 48);
            buf[forwardPosition++] = (byte) (ln >>> 40);
            buf[forwardPosition++] = (byte) (ln >>> 32);
            buf[forwardPosition++] = (byte) (ln >>> 24);
            buf[forwardPosition++] = (byte) (ln >>> 16);
            buf[forwardPosition++] = (byte) (ln >>> 8);
            buf[forwardPosition++] = (byte) ln;
        }
    }

    public void writeKeyDouble(Object dl) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(9);
        if (dl == null) {
            writeNull();
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
            buf[forwardPosition++] = 0;
        } else {
            writeNotNull();
            long ln = (Double.doubleToLongBits((Double) dl));
            if (ln >= 0) {
                buf[forwardPosition++] = (byte) (ln >>> 56 ^ 0x80);
                buf[forwardPosition++] = (byte) (ln >>> 48);
                buf[forwardPosition++] = (byte) (ln >>> 40);
                buf[forwardPosition++] = (byte) (ln >>> 32);
                buf[forwardPosition++] = (byte) (ln >>> 24);
                buf[forwardPosition++] = (byte) (ln >>> 16);
                buf[forwardPosition++] = (byte) (ln >>> 8);
                buf[forwardPosition++] = (byte) ln;
            } else {
                buf[forwardPosition++] = (byte) (~ ln >>> 56);
                buf[forwardPosition++] = (byte) (~ ln >>> 48);
                buf[forwardPosition++] = (byte) (~ ln >>> 40);
                buf[forwardPosition++] = (byte) (~ ln >>> 32);
                buf[forwardPosition++] = (byte) (~ ln >>> 24);
                buf[forwardPosition++] = (byte) (~ ln >>> 16);
                buf[forwardPosition++] = (byte) (~ ln >>> 8);
                buf[forwardPosition++] = (byte) ~ ln;
            }
        }
    }

    public void writeBytes(Object bytes) throws IndexOutOfBoundsException, ClassCastException {
        if (bytes == null) {
            ensureRemainder(1);
            writeNull();
        } else if (((byte[]) bytes).length == 0) {
            ensureRemainder(5);
            writeNotNull();
            writeLength(0);
        } else {
            internWriteBytes((byte[]) bytes);
        }
    }

    public void updateBytes(Object bytes) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipBytes();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeBytes(bytes);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipBytes() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(readLength());
        }
    }

    public void writeKeyBytes(Object bytes) throws IndexOutOfBoundsException, ClassCastException {
        if (bytes == null) {
            ensureRemainder(1);
            writeNull();
        } else if (((byte[]) bytes).length == 0) {
            ensureRemainder(1);
            writeNotNull();
            writeKeyLength(0);
        } else {
            internWriteKeyBytes((byte[]) bytes);
        }
    }

    public void updateKeyBytes(Object bytes) throws IndexOutOfBoundsException, ClassCastException {
        final int startMark = forwardPosition;
        skipKeyBytes();
        reversePosition += 4;
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeKeyBytes(bytes);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipKeyBytes() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(readKeyLength());
        }
    }

    public void writeString(Object string) throws IndexOutOfBoundsException, ClassCastException {
        if (string == null) {
            ensureRemainder(1);
            writeNull();
        } else if (((String) string).length() == 0) {
            ensureRemainder(5);
            writeNotNull();
            writeLength(0);
        } else {
            byte[] value = ((String) string).getBytes(StandardCharsets.UTF_8);
            internWriteBytes(value);
        }
    }

    public void updateString(Object string) throws IndexOutOfBoundsException, ClassCastException {
        final int startMark = forwardPosition;
        skipString();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeString(string);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipString() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(readLength());
        }
    }

    public void writeKeyString(Object string) throws IndexOutOfBoundsException, ClassCastException {
        if (string == null) {
            ensureRemainder(1);
            writeNull();
        } else if (((String) string).length() == 0) {
            ensureRemainder(1);
            writeNotNull();
            writeKeyLength(0);
        } else {
            byte[] value = ((String) string).getBytes(StandardCharsets.UTF_8);
            internWriteKeyBytes(value);
        }
    }

    public void updateKeyString(Object string) throws IndexOutOfBoundsException, ClassCastException {
        final int startMark = forwardPosition;
        skipKeyString();
        reversePosition += 4;
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeKeyString(string);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipKeyString() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(readKeyLength());
        }
    }

    public void writeBooleanList(Object booleanList) throws IndexOutOfBoundsException, ClassCastException {
        if (booleanList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Boolean> list = (List<Boolean>) booleanList;
            ensureRemainder(5 + (2 * list.size()));
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeBoolean(list.get(i));
            }
        }
    }

    public void updateBooleanList(Object booleanList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipBooleanList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeBooleanList(booleanList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipBooleanList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(2 * readLength());
        }
    }

    public void writeShortList(Object shortList) throws IndexOutOfBoundsException, ClassCastException {
        if (shortList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Short> list = (List<Short>) shortList;
            ensureRemainder(5 + (3 * list.size()));
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeShort(list.get(i));
            }
        }
    }

    public void updateShortList(Object shortList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipShortList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeShortList(shortList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipShortList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(3 * readLength());
        }
    }

    public void writeIntegerList(Object integerList) throws IndexOutOfBoundsException, ClassCastException {
        if (integerList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Integer> list = (List<Integer>) integerList;
            ensureRemainder(5 + (5 * list.size()));
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeInt(list.get(i));
            }
        }
    }

    public void updateIntegerList(Object integerList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipIntegerList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeIntegerList(integerList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipIntegerList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(5 * readLength());
        }
    }

    public void writeFloatList(Object floatList) throws IndexOutOfBoundsException, ClassCastException {
        if (floatList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Float> list = (List<Float>) floatList;
            ensureRemainder(5 + (5 * list.size()));
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeFloat(list.get(i));
            }
        }
    }

    public void updateFloatList(Object floatList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipFloatList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeFloatList(floatList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipFloatList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(5 * readLength());
        }
    }

    public void writeLongList(Object longList) throws IndexOutOfBoundsException, ClassCastException {
        if (longList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Long> list = (List<Long>) longList;
            ensureRemainder(5 + (9 * list.size()));
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeLong(list.get(i));
            }
        }
    }

    public void updateLongList(Object longList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipLongList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeLongList(longList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipLongList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(9 * readLength());
        }
    }

    public void writeDoubleList(Object doubleList) throws IndexOutOfBoundsException, ClassCastException {
        if (doubleList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<Double> list = (List<Double>) doubleList;
            ensureRemainder(5 + (9 * list.size()));
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeDouble(list.get(i));
            }
        }
    }

    public void updateDoubleList(Object doubleList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipDoubleList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeDoubleList(doubleList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipDoubleList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(9 * readLength());
        }
    }

    public void writeBytesList(Object bytesList) throws IndexOutOfBoundsException, ClassCastException {
        if (bytesList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<byte[]> list = (List<byte[]>) bytesList;
            ensureRemainder(5);
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeBytes(list.get(i));
            }
        }
    }

    public void updateBytesList(Object bytesList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipBytesList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeBytesList(bytesList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipBytesList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            int size = readLength();
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    skipBytes();
                }
            }
        }
    }

    public void writeStringList(Object stringList) throws IndexOutOfBoundsException, ClassCastException {
        if (stringList == null) {
            ensureRemainder(1);
            writeNull();
        } else {
            List<String> list = (List<String>) stringList;
            ensureRemainder(5);
            writeNotNull();
            writeLength(list.size());
            for (int i = 0; i < list.size(); i++) {
                writeString(list.get(i));
            }
        }
    }

    public void updateStringList(Object stringList) throws IndexOutOfBoundsException, ClassCastException {
        int startMark = forwardPosition;
        skipStringList();
        int remainBytesLength = buf.length - forwardPosition;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, forwardPosition, buf.length);
        }
        forwardPosition = startMark;
        writeStringList(stringList);
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, forwardPosition, remainBytesLength);
        }
    }

    public void skipStringList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            int size = readLength();
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    skipString();
                }
            }
        }
    }

    private void writeNull() throws IndexOutOfBoundsException {
        buf[forwardPosition++] = 0;
    }

    private void writeNotNull() throws IndexOutOfBoundsException {
        buf[forwardPosition++] = 1;
    }

    private void writeLength(int length) throws IndexOutOfBoundsException {
        buf[forwardPosition++] = (byte) (length >> 24);
        buf[forwardPosition++] = (byte) (length >> 16);
        buf[forwardPosition++] = (byte) (length >> 8);
        buf[forwardPosition++] = (byte) length;
    }

    private void writeKeyLength(int length) throws IndexOutOfBoundsException {
        lengthBuf[reversePosition--] = (byte) (length >>> 24);
        lengthBuf[reversePosition--] = (byte) (length >>> 16);
        lengthBuf[reversePosition--] = (byte) (length >>> 8);
        lengthBuf[reversePosition--] = (byte) length;
    }

    private void internWriteBytes(byte[] value) throws IndexOutOfBoundsException, ClassCastException {
        ensureRemainder(5 + value.length);
        writeNotNull();
        writeLength(value.length);
        System.arraycopy(value, 0, buf, forwardPosition, value.length);
        forwardPosition += value.length;
    }

    private void internWriteKeyBytes(byte[] value) throws IndexOutOfBoundsException, ClassCastException {
        int groupNum = value.length / 8;
        int size = (groupNum + 1) * 9;
        int reminderSize = value.length % 8;
        int remindZero;
        if (reminderSize == 0) {
            reminderSize = 8;
            remindZero = 8;
        } else {
            remindZero = 8 - reminderSize;
        }
        ensureRemainder(1 + size);
        writeNotNull();
        writeKeyLength(size);
        for (int i = 0; i < groupNum; i++) {
            System.arraycopy(value, 8 * i, buf, forwardPosition, 8);
            forwardPosition += 8;
            buf[forwardPosition++] = (byte) 255;
        }
        if (reminderSize < 8) {
            System.arraycopy(value, 8 * groupNum, buf, forwardPosition, reminderSize);
            forwardPosition += reminderSize;
        }
        for (int i = 0; i < remindZero; i++) {
            buf[forwardPosition++] = 0;
        }
        buf[forwardPosition++] = (byte) (255 - remindZero);
    }

    public void skipByte() {
        skip(1);
    }

    private boolean readIsNull() throws IndexOutOfBoundsException {
        return buf[forwardPosition++] == 0;
    }

    public byte read() {
        return buf[forwardPosition++];
    }

    public Short readShort() throws IndexOutOfBoundsException, ClassCastException {
        if (readIsNull()) {
            forwardPosition += 2;
            return null;
        }
        return (short) (((buf[forwardPosition++] & 0xFF) << 8)
            | buf[forwardPosition++] & 0xFF);
    }

    private int readLength() throws IndexOutOfBoundsException {
        return (((buf[forwardPosition++] & 0xFF) << 24)
            | ((buf[forwardPosition++] & 0xFF) << 16)
            | ((buf[forwardPosition++] & 0xFF) << 8)
            | buf[forwardPosition++] & 0xFF);
    }

    private int readKeyLength() throws IndexOutOfBoundsException {
        return (((lengthBuf[reversePosition--] & 0xFF) << 24)
            | ((lengthBuf[reversePosition--] & 0xFF) << 16)
            | ((lengthBuf[reversePosition--] & 0xFF) << 8)
            | lengthBuf[reversePosition--] & 0xFF);
    }

    private void skipKeyLength() {
        reversePosition -= 4;
    }

    public void skip(int length) {
        forwardPosition += length;
    }

    private void ensureRemainder(int length) {
        if (buf.length - forwardPosition < length) {
            buf = Arrays.copyOf(buf, forwardPosition + length);
        }
    }

    public byte[] getByteArray() {
        if (lengthBuf != null && lengthBuf.length > 0) {
            ensureRemainder(lengthBuf.length);
            System.arraycopy(lengthBuf, 0, buf, forwardPosition, lengthBuf.length);
            forwardPosition += lengthBuf.length;
        }
        if (forwardPosition != buf.length) {
            buf = Arrays.copyOf(buf, forwardPosition);
        }
        return buf;
    }

    public byte[] getByteArrayWithoutLength() {
        if (forwardPosition != buf.length) {
            buf = Arrays.copyOf(buf, forwardPosition);
        }
        return buf;
    }
}
