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

package io.dingodb.common.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public final class VarNumberCodec {
    public static final int INT_MAX_LEN = 5;
    public static final int LONG_MAX_LEN = 10;

    private VarNumberCodec() {
    }

    /**
     * Calculate the size of the {@code value} encoded with VarInt.
     */
    public static int computeVarIntSize(int value) {
        int count = 0;
        while (true) {
            if ((value & ~0x7F) == 0) {
                count++;
                return count;
            } else {
                count++;
                value >>>= 7;
            }
        }
    }

    /**
     * Encode {@code value} using VarInt.
     */
    public static byte[] encodeVarInt(int value) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(5);
        while ((value & ~0x7F) != 0) {
            outputStream.write((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        outputStream.write((byte) value);
        return outputStream.toByteArray();
    }

    /**
     * Encode {@code value} using VarInt.
     */
    public static byte[] encodeVarLong(long value) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(LONG_MAX_LEN);
        while ((value & ~0x7F) != 0) {
            outputStream.write((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        outputStream.write((byte) value);
        return outputStream.toByteArray();
    }

    /**
     * Encode {@code value} using ZigZagInt.
     */
    public static byte[] encodeZigZagInt(int value) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(5);
        value = (value << 1) ^ (value >> 31);
        if ((value & ~Byte.MAX_VALUE) != 0) {
            outputStream.write((byte) ((value | 0x80) & 0xFF));
            value >>>= 7;
            if (value > Byte.MAX_VALUE) {
                outputStream.write((byte) ((value | 0x80) & 0xFF));
                value >>>= 7;
                if (value > Byte.MAX_VALUE) {
                    outputStream.write((byte) ((value | 0x80) & 0xFF));
                    value >>>= 7;
                    if (value > Byte.MAX_VALUE) {
                        outputStream.write((byte) ((value | 0x80) & 0xFF));
                        value >>>= 7;
                    }
                }
            }
        }
        outputStream.write(((byte) value));
        return outputStream.toByteArray();
    }

    /**
     * Read int from {@code bytes}, and use VarInt load.
     */
    public static Integer readVarInt(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        int position = 0;
        int b = Byte.MAX_VALUE + 1;
        int result = 0;
        int maxBytes = INT_MAX_LEN;
        while ((maxBytes >= 0) && b > Byte.MAX_VALUE) {
            result ^= ((b = (bytes[position++] & 0XFF)) & 0X7F) << ((INT_MAX_LEN - maxBytes--) * (Byte.SIZE - 1));
        }
        return result;
    }

    /**
     * Read int from {@code bytes}, and use VarInt load.
     */
    public static Integer readVarInt(ByteBuffer buf) {
        int readerIndex = buf.position();
        int maxBytes = INT_MAX_LEN;
        int b = Byte.MAX_VALUE + 1;
        int result = 0;
        while ((maxBytes >= 0) && b > Byte.MAX_VALUE) {
            if (!buf.hasRemaining()) {
                buf.position(readerIndex);
                return null;
            }
            result ^= ((b = (buf.get() & 0XFF)) & 0X7F) << ((INT_MAX_LEN - maxBytes--) * (Byte.SIZE - 1));
        }
        return result;
    }

    /**
     * Read int from {@code bytes}, and use VarInt load.
     */
    public static Integer readVarInt(ByteArrayInputStream bais) {
        bais.mark(0);
        int maxBytes = INT_MAX_LEN;
        int b = Byte.MAX_VALUE + 1;
        int result = 0;
        while ((maxBytes >= 0) && b > Byte.MAX_VALUE) {
            if (bais.available() < 1) {
                bais.reset();
                return null;
            }
            result ^= ((b = (bais.read() & 0XFF)) & 0X7F) << ((INT_MAX_LEN - maxBytes--) * (Byte.SIZE - 1));
        }
        return result;
    }

    /**
     * Read long from {@code bytes}, and use VarLong load.
     */
    public static Long readVarLong(byte[] bytes) {
        int position = 0;
        int maxBytes = LONG_MAX_LEN;
        long b = Byte.MAX_VALUE + 1;
        long result = 0;
        while ((maxBytes >= 0) && b > Byte.MAX_VALUE) {
            result ^= ((b = (bytes[position++] & 0XFF)) & 0X7F) << ((LONG_MAX_LEN - maxBytes--) * (Byte.SIZE - 1));
        }
        return result;
    }

    /**
     * Read long from {@code buffer}, and use VarLong load.
     */
    public static Long readVarLong(ByteBuffer buf) {
        int readerIndex = buf.position();
        int maxBytes = LONG_MAX_LEN;
        long b = Byte.MAX_VALUE + 1;
        long result = 0;
        while ((maxBytes >= 0) && b > Byte.MAX_VALUE) {
            if (!buf.hasRemaining()) {
                buf.position(readerIndex);
                return null;
            }
            result ^= ((b = (buf.get() & 0XFF)) & 0X7F) << ((LONG_MAX_LEN - maxBytes--) * (Byte.SIZE - 1));
        }
        return result;
    }

    /**
     * Read int from {@code bytes}, and use ZigZagIng load.
     */
    public static int readZigZagInt(byte[] bytes) {
        int position = 0;
        int b = Byte.MAX_VALUE + 1;
        int result = 0;
        int maxBytes = INT_MAX_LEN;
        while ((maxBytes >= 0) && b > Byte.MAX_VALUE) {
            result ^= ((b = (bytes[position++] & 0XFF)) & 0X7F) << ((INT_MAX_LEN - maxBytes--) * (Byte.SIZE - 1));
        }
        return result >>> 1 ^ -(result & 1);
    }

    /**
     * Read int from {@code buf}, and use ZigZagIng load.
     */
    public static Integer readZigZagInt(ByteBuffer buf) {
        int readerIndex = buf.position();
        int maxBytes = INT_MAX_LEN;
        int b = Byte.MAX_VALUE + 1;
        int result = 0;
        while ((maxBytes >= 0) && b > Byte.MAX_VALUE) {
            if (!buf.hasRemaining()) {
                buf.position(readerIndex);
                return null;
            }
            result ^= ((b = (buf.get() & 0XFF)) & 0X7FL) << ((INT_MAX_LEN - maxBytes--) * (Byte.SIZE - 1));
        }
        return result >>> 1 ^ -(result & 1);
    }

}
