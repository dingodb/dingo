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

import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Pair;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.IntStream;

import static io.dingodb.common.codec.VarNumberCodec.encodeVarInt;

public final class PrimitiveCodec {

    private static final int[] ascIntOffsets = IntStream.iterate(0, i -> i + 1).limit(Integer.BYTES).toArray();
    private static final int[] descIntOffsets = IntStream.iterate(3, i -> i - 1).limit(Integer.BYTES).toArray();
    private static final int[] ascLongOffsets = IntStream.iterate(0, i -> i + 1).limit(Long.BYTES).toArray();
    private static final int[] descLongOffsets = IntStream.iterate(7, i -> i - 1).limit(Long.BYTES).toArray();

    private PrimitiveCodec() {
    }

    public static byte[] encodeInt(int value) {
        return encodeInt(value, new byte[Integer.BYTES], 0, true);
    }

    public static byte[] encodeInt(int value, boolean bigEnd) {
        return encodeInt(value, new byte[Integer.BYTES], 0, bigEnd);
    }

    public static byte[] encodeInt(int value, byte[] target) {
        return encodeInt(value, target, target.length - Integer.BYTES, true);
    }

    public static byte[] encodeInt(int value, byte[] target, boolean bigEnd) {
        return encodeInt(value, target, target.length - Integer.BYTES, bigEnd);
    }

    public static byte[] encodeInt(int value, byte[] target, int index) {
        return encodeInt(value, target, index, true);
    }

    public static byte[] encodeInt(int value, byte[] target, int index, boolean bigEnd) {
        int[] offsets = bigEnd ? ascIntOffsets : descIntOffsets;
        target[index + offsets[0]] = (byte) (value >> 24);
        target[index + offsets[1]] = (byte) (value >> 16);
        target[index + offsets[2]] = (byte) (value >> 8);
        target[index + offsets[3]] = (byte) (value);
        return target;
    }

    public static byte[] encodeLong(long value) {
        return encodeLong(value, new byte[Long.BYTES], 0, true);
    }

    public static byte[] encodeLong(long value, boolean bigEnd) {
        return encodeLong(value, new byte[Long.BYTES], 0, bigEnd);
    }

    public static byte[] encodeLong(long value, byte[] target) {
        return encodeLong(value, target, target.length - Long.BYTES, true);
    }

    public static byte[] encodeLong(long value, byte[] target, boolean bigEnd) {
        return encodeLong(value, target, target.length - Long.BYTES, bigEnd);
    }

    public static byte[] encodeLong(long value, byte[] target, int index) {
        return encodeLong(value, target, index, true);
    }

    public static byte[] encodeLong(long value, byte[] target, int index, boolean bigEnd) {
        int[] offsets = bigEnd ? ascLongOffsets : descLongOffsets;
        target[index + offsets[0]] = (byte) (value >> 56);
        target[index + offsets[1]] = (byte) (value >> 48);
        target[index + offsets[2]] = (byte) (value >> 40);
        target[index + offsets[3]] = (byte) (value >> 32);
        target[index + offsets[4]] = (byte) (value >> 24);
        target[index + offsets[5]] = (byte) (value >> 16);
        target[index + offsets[6]] = (byte) (value >> 8);
        target[index + offsets[7]] = (byte) (value);
        return target;
    }

    public static Integer decodeInt(byte[] bytes) {
        return decodeInt(bytes, 0, true);
    }

    public static Integer decodeInt(byte[] bytes, boolean bigEnd) {
        return decodeInt(bytes, 0, bigEnd);
    }

    public static Integer decodeInt(byte[] bytes, int index) {
        return decodeInt(bytes, index, true);
    }

    public static Integer decodeInt(byte[] bytes, int index, boolean bigEnd) {
        if (bytes == null) {
            return null;
        }
        int[] offsets = bigEnd ? ascIntOffsets : descIntOffsets;
        return (bytes[index + offsets[0]] & 0xFF) << 24
            |  (bytes[index + offsets[1]] & 0xFF) << 16
            |  (bytes[index + offsets[2]] & 0xFF) << 8
            |   bytes[index + offsets[3]] & 0xFF;
    }

    public static Long decodeLong(byte[] bytes) {
        return decodeLong(bytes, 0, true);
    }

    public static Long decodeLong(byte[] bytes, boolean bigEnd) {
        return decodeLong(bytes, 0, bigEnd);
    }

    public static Long decodeLong(byte[] bytes, int index) {
        return decodeLong(bytes, index, true);
    }

    public static Long decodeLong(byte[] bytes, int index, boolean bigEnd) {
        if (bytes == null) {
            return null;
        }
        int[] offsets = bigEnd ? ascLongOffsets : descLongOffsets;
        return (bytes[index + offsets[0]] & 0xFFL) << 56
            |  (bytes[index + offsets[1]] & 0xFFL) << 48
            |  (bytes[index + offsets[2]] & 0xFFL) << 40
            |  (bytes[index + offsets[3]] & 0xFFL) << 32
            |  (bytes[index + offsets[4]] & 0xFFL) << 24
            |  (bytes[index + offsets[5]] & 0xFFL) << 16
            |  (bytes[index + offsets[6]] & 0xFFL) << 8
            |   bytes[index + offsets[7]] & 0xFFL;
    }

    public static byte[] encodeString(String str) {
        if (str == null) {
            return new byte[] {0};
        }
        byte[] content = str.getBytes(StandardCharsets.UTF_8);
        byte[] len = encodeVarInt(content.length);
        return ByteArrayUtils.concatByteArray(len, content);
    }


    public static String readString(byte[] bytes) {
        Integer len = VarNumberCodec.readVarInt(bytes);
        if (len == null || len < 0) {
            return null;
        }
        return new String(bytes, VarNumberCodec.computeVarIntSize(len), len);
    }

    public static String readString(ByteBuffer buf) {
        int readerIndex = buf.position();
        Integer len = VarNumberCodec.readVarInt(buf);
        if (len == null || len < 0 || buf.limit() - buf.position() < len) {
            buf.position(readerIndex);
            return null;
        }
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes);
    }

    public static byte[] encodeArray(final byte[] content) {
        if (content == null) {
            return new byte[] {0};
        }
        byte[] len = encodeVarInt(content.length);
        return ByteArrayUtils.concatByteArray(len, content);
    }

    public static Pair<byte[], Integer> decodeArray(final byte[] bytes, final int offset) {
        byte[] lenBytes = Arrays.copyOfRange(bytes, offset, offset + 4);
        Integer len = VarNumberCodec.readVarInt(lenBytes);
        if (len == null || len < 0) {
            return null;
        }
        int lenOffset = VarNumberCodec.computeVarIntSize(len);
        int totalOffset = offset + lenOffset + len;
        return new Pair<>(Arrays.copyOfRange(bytes, offset + lenOffset, totalOffset), lenOffset + len);
    }
}
