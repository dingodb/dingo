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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BinaryDecoder {
    private final byte[] buf;
    private int position = 0;

    public BinaryDecoder(byte[] buf) {
        this.buf = buf;
    }

    public boolean readIsNull() throws IndexOutOfBoundsException {
        return buf[position++] == 0;
    }

    public Boolean readBoolean() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            position++;
            return null;
        }
        return buf[position++] != 0;
    }

    public Short readShort() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            position += 2;
            return null;
        }
        return (short) (((buf[position++] & 0xFF) << 8)
            | ((buf[position++] & 0xFF) << 0));

    }

    public Integer readInt() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            position += 4;
            return null;
        }
        return readIntNoNull();
    }

    public int readIntNoNull() throws IndexOutOfBoundsException {
        return (((buf[position++] & 0xFF) << 24)
            | ((buf[position++] & 0xFF) << 16)
            | ((buf[position++] & 0xFF) << 8)
            | ((buf[position++] & 0xFF) << 0));
    }

    public Long readLong() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            position += 8;
            return null;
        }
        long l = 0;
        for (int i = 0; i < 8; i++) {
            l <<= 8;
            l |= buf[position++] & 0xFF;
        }
        return l;
    }

    public Float readFloat() throws IndexOutOfBoundsException {
        Integer i = readInt();
        if (i == null) {
            return null;
        }
        return Float.intBitsToFloat(i);
    }

    public Double readDouble() throws IndexOutOfBoundsException {
        Long l = readLong();
        if (l == null) {
            return null;
        }
        return Double.longBitsToDouble(l);
    }

    public String readString() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int length = readIntNoNull();
            if (length > 0) {
                byte[] buf = new byte[length];
                System.arraycopy(this.buf, position, buf, 0, length);
                skip(length);
                return new String(buf, StandardCharsets.UTF_8);
            }
            return "";
        }
    }

    public ByteBuffer readBytes() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int length = readIntNoNull();
            if (length > 0) {
                byte[] buf = new byte[length];
                System.arraycopy(this.buf, position, buf, 0, length);
                skip(length);
                return ByteBuffer.wrap(buf);
            }
            return ByteBuffer.allocate(0);
        }
    }

    public List<Boolean> readBooleanList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readIntNoNull();
            List<Boolean> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readBoolean());
            }
            return list;
        }
    }

    public List<Short> readShortList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readIntNoNull();
            List<Short> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readShort());
            }
            return list;
        }
    }

    public List<Integer> readIntegerList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readIntNoNull();
            List<Integer> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readInt());
            }
            return list;
        }
    }

    public List<Float> readFloatList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readIntNoNull();
            List<Float> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readFloat());
            }
            return list;
        }
    }

    public List<Long> readLongList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readIntNoNull();
            List<Long> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readLong());
            }
            return list;
        }
    }

    public List<Double> readDoubleList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readIntNoNull();
            List<Double> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readDouble());
            }
            return list;
        }
    }

    public List<String> readStringList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readIntNoNull();
            List<String> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readString());
            }
            return list;
        }
    }

    public List<byte[]> readBytesList() throws IndexOutOfBoundsException {
        if (readIsNull()) {
            return null;
        } else {
            int size = readIntNoNull();
            List<byte[]> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(readBytes().array());
            }
            return list;
        }
    }

    public void skipBooleanList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(2 * readIntNoNull());
        }
    }

    public void skipShortList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(3 * readIntNoNull());
        }
    }

    public void skipIntegerList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(5 * readIntNoNull());
        }
    }

    public void skipFloatList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(5 * readIntNoNull());
        }
    }

    public void skipLongList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(9 * readIntNoNull());
        }
    }

    public void skipDoubleList() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(9 * readIntNoNull());
        }
    }

    public void skipString() throws IndexOutOfBoundsException {
        if (!readIsNull()) {
            skip(readIntNoNull());
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

    public void skip(int length) throws IndexOutOfBoundsException {
        position += length;
    }

    public int remainder() {
        return buf.length - position;
    }
}
