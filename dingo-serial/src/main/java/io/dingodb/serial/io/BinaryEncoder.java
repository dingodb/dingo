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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class BinaryEncoder {
    private byte[] buf;
    private int position = 0;

    public BinaryEncoder(int limit) {
        this.buf = new byte[limit];
    }

    public BinaryEncoder(byte[] buf) {
        this.buf = buf;
    }

    public BinaryEncoder(byte[] buf, int position) {
        this.buf = buf;
        this.position = position;
    }

    public boolean readIsNull() throws IOException {
        return buf[position++] == 0 ? true : false;
    }

    public Short readShort() throws IOException {
        if (readIsNull()) {
            position+=2;
            return null;
        }
        return (short) (((buf[position++] & 0xFF) << 8) |
            ((buf[position++] & 0xFF) << 0));

    }

    public int readIntNoNull() throws IOException {
        return (((buf[position++] & 0xFF) << 24) |
            ((buf[position++] & 0xFF) << 16) |
            ((buf[position++] & 0xFF) << 8) |
            ((buf[position++] & 0xFF) << 0));
    }

    private void writeNull() throws IOException {
        buf[position++] = 0;
    }

    private void writeNotNull() throws IOException {
        buf[position++] = 1;
    }

    public void writeBoolean(Object b) throws IOException, ClassCastException {
        if (b == null) {
            writeNull();
            buf[position++] = 0;
        } else {
            writeNotNull();
            buf[position++] = (byte) ((Boolean) b ? 1 : 0);
        }
    }

    public void writeShort(Object s) throws IOException, ClassCastException {
        if (s == null) {
            writeNull();
            buf[position++] = 0;
            buf[position++] = 0;
        } else {
            writeNotNull();
            buf[position++] = (byte) ((Short) s >>> 8);
            buf[position++] = (byte) ((Short) s >>> 0);
        }
    }

    public void writeInt(Object n) throws IOException, ClassCastException {
        if (n == null) {
            writeNull();
            buf[position++] = 0;
            buf[position++] = 0;
            buf[position++] = 0;
            buf[position++] = 0;
        } else {
            writeNotNull();
            buf[position++] = (byte) ((Integer) n >>> 24);
            buf[position++] = (byte) ((Integer) n >>> 16);
            buf[position++] = (byte) ((Integer) n >>> 8);
            buf[position++] = (byte) ((Integer) n >>> 0);
        }
    }

    public void writeLong(Object l) throws IOException, ClassCastException {
        if (l == null) {
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
            buf[position++] = (byte) ((Long) l >>> 56);
            buf[position++] = (byte) ((Long) l >>> 48);
            buf[position++] = (byte) ((Long) l >>> 40);
            buf[position++] = (byte) ((Long) l >>> 32);
            buf[position++] = (byte) ((Long) l >>> 24);
            buf[position++] = (byte) ((Long) l >>> 16);
            buf[position++] = (byte) ((Long) l >>> 8);
            buf[position++] = (byte) ((Long) l >>> 0);
        }
    }

    public void writeFloat(Object f) throws IOException, ClassCastException {
        if (f == null) {
            writeInt(null);
        } else {
            writeInt(Float.floatToIntBits((Float) f));
        }
    }

    public void writeDouble(Object d) throws IOException, ClassCastException {
        if (d == null) {
            writeLong(null);
        } else {
            writeLong(Double.doubleToLongBits((Double) d));
        }
    }

    public void writeString(Object string) throws IOException, ClassCastException {
        writeString(string, 0);
    }

    public void writeString(Object string, int maxLength) throws IOException, ClassCastException {
        if (string == null) {
            ensureRemainder(1);
            writeNull();
        } else if (((String) string).length() == 0) {
            ensureRemainder(5);
            writeInt(0);
        } else {
            byte[] value = ((String) string).getBytes(StandardCharsets.UTF_8);
            int length = maxLength == 0 ? value.length : maxLength - 5;
            ensureRemainder(length + 5);
            writeInt(length);
            System.arraycopy(value, 0, buf, position, length);
            position += length;
        }
    }

    public void writeBytes(Object bytes) throws IOException, ClassCastException {
        writeBytes(bytes, 0);
    }

    public void writeBytes(Object bytes, int maxLength) throws IOException, ClassCastException {
        if (bytes == null) {
            ensureRemainder(1);
            writeNull();
        } else if (((ByteBuffer) bytes).capacity() == 0) {
            ensureRemainder(5);
            writeInt(0);
        } else {
            byte[] value = ((ByteBuffer) bytes).array();
            int length = maxLength == 0 ? value.length : maxLength - 5;
            ensureRemainder(length + 5);
            writeInt(length);
            System.arraycopy(value, 0, buf, position, length);
            position += length;
        }
    }

    public void updateString(Object string) throws IOException, ClassCastException {
        updateString(string, 0);
    }

    public void updateString(Object string, int maxLength) throws IOException, ClassCastException {
        int startMark = position;
        skipString();
        int remainBytesLength = buf.length - position;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, position, buf.length);
        }
        position = startMark;
        if (string == null) {
            ensureRemainder(1);
            writeNull();
        } else if (((String) string).length() == 0) {
            ensureRemainder(5);
            writeInt(0);
        } else {
            byte[] value = ((String) string).getBytes(StandardCharsets.UTF_8);
            int length = maxLength == 0 ? value.length : maxLength - 5;
            ensureRemainder(length + 5);
            writeInt(length);
            System.arraycopy(value, 0, buf, position, length);
            position += length;
        }
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, position, remainBytesLength);
        }
    }

    public void updateBytes(Object bytes) throws IOException, ClassCastException {
        updateBytes(bytes, 0);
    }

    public void updateBytes(Object bytes, int maxLength) throws IOException, ClassCastException {
        int startMark = position;
        skipString();
        int remainBytesLength = buf.length - position;
        byte[] remainBytes = null;
        if (remainBytesLength > 0) {
            remainBytes = Arrays.copyOfRange(buf, position, buf.length);
        }
        position = startMark;
        if (bytes == null) {
            ensureRemainder(1);
            writeNull();
        } else if (((ByteBuffer) bytes).capacity() == 0) {
            ensureRemainder(5);
            writeInt(0);
        } else {
            byte[] value = ((ByteBuffer) bytes).array();
            int length = maxLength == 0 ? value.length : maxLength - 5;
            ensureRemainder(length + 5);
            writeInt(length);
            System.arraycopy(value, 0, buf, position, length);
            position += length;
        }
        if (remainBytes != null) {
            ensureRemainder(remainBytesLength);
            System.arraycopy(remainBytes, 0, buf, position, remainBytesLength);
        }
    }

    public void skipString() throws IOException {
        if (!readIsNull()) {
            int length = readIntNoNull();
            if (length > 0) {
                skip(length);
            }
        }
    }

    public void skip(int n) {
        this.position += n;
    }

    private void ensureRemainder(int length) {
        if (buf.length - position < length)
            buf = Arrays.copyOf(buf, position + length);
    }

    public byte[] getByteArray() {
        if (position == buf.length) {
            return buf;
        }
        else {
            buf = Arrays.copyOf(buf, position);
            return buf;
        }
    }
}
