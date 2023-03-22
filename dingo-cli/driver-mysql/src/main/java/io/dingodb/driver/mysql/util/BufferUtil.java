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

package io.dingodb.driver.mysql.util;

import io.netty.buffer.ByteBuf;

public class BufferUtil {

    public static final void writeUB2(ByteBuf buffer, int operand) {
        buffer.writeByte(operand & 0xff);
        buffer.writeByte((byte) (operand >>> 8));
    }

    public static final void writeUB3(ByteBuf buffer, int operand) {
        buffer.writeByte((byte) (operand & 0xff));
        buffer.writeByte((byte) (operand >>> 8));
        buffer.writeByte((byte) (operand >>> 16));
    }

    public static final void writeInt(ByteBuf buffer, int operand) {
        buffer.writeByte((byte) (operand & 0xff));
        buffer.writeByte((byte) (operand >>> 8));
        buffer.writeByte((byte) (operand >>> 16));
        buffer.writeByte((byte) (operand >>> 24));
    }

    public static final void writeFloat(ByteBuf buffer, float operand) {
        writeInt(buffer, Float.floatToIntBits(operand));
    }

    public static final void writeUB4(ByteBuf buffer, long operand) {
        buffer.writeByte((byte) (operand & 0xff));
        buffer.writeByte((byte) (operand >>> 8));
        buffer.writeByte((byte) (operand >>> 16));
        buffer.writeByte((byte) (operand >>> 24));
    }

    public static final void writeLong(ByteBuf buffer, long operand) {
        buffer.writeByte((byte) (operand & 0xff));
        buffer.writeByte((byte) (operand >>> 8));
        buffer.writeByte((byte) (operand >>> 16));
        buffer.writeByte((byte) (operand >>> 24));
        buffer.writeByte((byte) (operand >>> 32));
        buffer.writeByte((byte) (operand >>> 40));
        buffer.writeByte((byte) (operand >>> 48));
        buffer.writeByte((byte) (operand >>> 56));
    }

    public static final void writeDouble(ByteBuf buffer, double operand) {
        writeLong(buffer, Double.doubleToLongBits(operand));
    }

    public static final void writeLength(ByteBuf buffer, long operand) {
        if (operand < 251) {
            buffer.writeByte((byte) operand);
        } else if (operand < 0x10000L) {
            buffer.writeByte((byte) 252);
            writeUB2(buffer, (int) operand);
        } else if (operand < 0x1000000L) {
            buffer.writeByte((byte) 253);
            writeUB3(buffer, (int) operand);
        } else {
            buffer.writeByte((byte) 254);
            writeLong(buffer, operand);
        }
    }

    public static final void writeWithNull(ByteBuf buffer, byte[] src) {
        buffer.writeBytes(src);
        buffer.writeByte((byte) 0);
    }

    public static final void writeWithLength(ByteBuf buffer, byte[] src) {
        int length = src.length;
        if (length < 251) {
            buffer.writeByte((byte) length);
        } else if (length < 0x10000L) {
            buffer.writeByte((byte) 252);
            writeUB2(buffer, length);
        } else if (length < 0x1000000L) {
            buffer.writeByte((byte) 253);
            writeUB3(buffer, length);
        } else {
            buffer.writeByte((byte) 254);
            writeLong(buffer, length);
        }
        buffer.writeBytes(src);
    }

    public static final void writeWithLength(ByteBuf buffer, byte[] src,
                                             byte nullValue) {
        if (src == null) {
            buffer.writeByte(nullValue);
        } else {
            writeWithLength(buffer, src);
        }
    }

    public static final int getLength(long length) {
        if (length < 251) {
            return 1;
        } else if (length < 0x10000L) {
            return 3;
        } else if (length < 0x1000000L) {
            return 4;
        } else {
            return 9;
        }
    }

    public static final int getLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            return 1 + length;
        } else if (length < 0x10000L) {
            return 3 + length;
        } else if (length < 0x1000000L) {
            return 4 + length;
        } else {
            return 9 + length;
        }
    }

}
