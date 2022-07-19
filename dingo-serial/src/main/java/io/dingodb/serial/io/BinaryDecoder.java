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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class BinaryDecoder {
    private byte[] buf;
    private int position = 0;

    public BinaryDecoder(byte[] buf) {
        this.buf = buf;
    }

    public boolean readIsNull() throws IOException {
        return buf[position++] == 0 ? true : false;
    }

    public Boolean readBoolean() throws IOException {
        if (readIsNull()) {
            position++;
            return null;
        }
        return buf[position++] == 0 ? false : true;
    }

    public Short readShort() throws IOException {
        if (readIsNull()) {
            position+=2;
            return null;
        }
        return (short) (((buf[position++] & 0xFF) << 8) |
            ((buf[position++] & 0xFF) << 0));

    }

    public Integer readInt() throws IOException {
        if (readIsNull()) {
            position+=4;
            return null;
        }
        return readIntNoNull();
    }

    public int readIntNoNull() throws IOException {
        return (((buf[position++] & 0xFF) << 24) |
            ((buf[position++] & 0xFF) << 16) |
            ((buf[position++] & 0xFF) << 8) |
            ((buf[position++] & 0xFF) << 0));
    }

    public Long readLong() throws IOException {
        if (readIsNull()) {
            position+=8;
            return null;
        }
        long l = 0;
        for (int i = 0; i < 8; i++) {
            l <<= 8;
            l |= buf[position++] & 0xFF;
        }
        return l;
    }

    public Float readFloat() throws IOException {
        Integer i = readInt();
        if (i == null) {
            return null;
        }
        return Float.intBitsToFloat(i);
    }

    public Double readDouble() throws IOException {
        Long l = readLong();
        if (l == null) {
            return null;
        }
        return Double.longBitsToDouble(l);
    }

    public String readString() throws IOException {
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

    public ByteBuffer readBytes() throws IOException {
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

    public void skip(int length) throws IOException {
        position+=length;
    }

    public int remainder() {
        return buf.length - position;
    }
}
