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

import io.dingodb.common.util.Pair;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class PrimitiveCodec {

    public static final int INT_MAX_LEN = 5;
    public static final int LONG_MAX_LEN = 10;

    private PrimitiveCodec() {
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

    public static byte[] encodeInt(int value) {
        byte[] result = new byte[4];
        result[0] = (byte) (value >> 24);
        result[1] = (byte) (value >> 16);
        result[2] = (byte) (value >> 8);
        result[3] = (byte) (value);
        return result;
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

    public static Integer readInt(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return bytes[3] & 0xFF | (bytes[2] & 0xFF) << 8 | (bytes[1] & 0xFF) << 16 | (bytes[0] & 0xFF) << 24;
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

    /**
     * Read int from {@code buf}, and use ZigZagIng load.
     */
    public static Integer readZigZagInt(ByteArrayInputStream bais) {
        bais.mark(0);
        int maxBytes = INT_MAX_LEN;
        int b = Byte.MAX_VALUE + 1;
        int result = 0;
        while ((maxBytes >= 0) && b > Byte.MAX_VALUE) {
            if (bais.available() < 1) {
                bais.reset();
                return null;
            }
            result ^= ((b = (bais.read() & 0XFF)) & 0X7FL) << ((INT_MAX_LEN - maxBytes--) * (Byte.SIZE - 1));
        }
        return result >>> 1 ^ -(result & 1);
    }

    public static byte[] encodeString(String str) {
        if (str == null) {
            return new byte[] {0};
        }
        byte[] content = str.getBytes(StandardCharsets.UTF_8);
        byte[] len = encodeVarInt(content.length);
        byte[] result = new byte[len.length + content.length];
        System.arraycopy(len, 0, result, 0, len.length);
        System.arraycopy(content, 0, result, len.length, content.length);
        return result;
    }

    public static String readString(byte[] bytes) {
        Integer len = readVarInt(bytes);
        if (len == null || len < 0) {
            return null;
        }
        return new String(bytes, computeVarIntSize(len), len);
    }

    public static String readString(ByteBuffer buf) {
        int readerIndex = buf.position();
        Integer len = readVarInt(buf);
        if (len == null || len < 0 || buf.limit() - buf.position() < len) {
            buf.position(readerIndex);
            return null;
        }
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes);
    }

    public static String readString(ByteArrayInputStream bais) {
        bais.mark(0);
        Integer len = readVarInt(bais);
        if (len == null || len < 0 || bais.available() < len) {
            bais.reset();
            return null;
        }
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) bais.read();
        }
        return new String(bytes);
    }

    public static byte[] encodeArray(final byte[] content) {
        if (content == null) {
            return new byte[] {0};
        }
        byte[] len = encodeVarInt(content.length);
        byte[] result = new byte[len.length + content.length];
        System.arraycopy(len, 0, result, 0, len.length);
        System.arraycopy(content, 0, result, len.length, content.length);
        return result;
    }

    public static Pair<byte[], Integer> decodeArray(final byte[] bytes, final int offset) {
        byte[] lenBytes = Arrays.copyOfRange(bytes, offset, offset + 4);
        Integer len = readVarInt(lenBytes);
        if (len == null || len < 0) {
            return null;
        }
        int lenOffset = computeVarIntSize(len);
        int totalOffset = offset + lenOffset + len;
        return new Pair(Arrays.copyOfRange(bytes, offset + lenOffset, totalOffset), lenOffset + len);
    }

    public static byte[] intToByteArray(int num) {
        return new byte[] {(byte)((num >> 24) & 0xff),(byte)((num >> 16) & 0xff),(byte)((num >> 8) & 0xff),
            (byte)(num & 0xff)};
    }

    public static int byteArrayToInt(byte[] arr) {
        return (arr[0] & 0xff) << 24 | (arr[1] & 0xff) << 16 | (arr[2] & 0xff) << 8 | (arr[3] & 0xff);
    }
}
