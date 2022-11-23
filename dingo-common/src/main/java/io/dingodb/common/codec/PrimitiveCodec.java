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

import static io.dingodb.common.codec.VarNumberCodec.encodeVarInt;

public final class PrimitiveCodec {

    private PrimitiveCodec() {
    }

    public static byte[] encodeInt(int value) {
        return encodeInt(value, new byte[4], 0);
    }

    public static byte[] encodeInt(int value, byte[] target, int index) {
        target[index] = (byte) (value >> 24);
        target[index + 1] = (byte) (value >> 16);
        target[index + 2] = (byte) (value >> 8);
        target[index + 3] = (byte) (value);
        return target;
    }

    public static byte[] encodeLong(long value) {
        return encodeLong(value, new byte[8], 0);
    }

    public static byte[] encodeLong(long value, byte[] target, int index) {
        target[index] = (byte) (value >> 56);
        target[index + 1] = (byte) (value >> 48);
        target[index + 2] = (byte) (value >> 40);
        target[index + 3] = (byte) (value >> 32);
        target[index + 4] = (byte) (value >> 24);
        target[index + 5] = (byte) (value >> 16);
        target[index + 6] = (byte) (value >> 8);
        target[index + 7] = (byte) (value);
        return target;
    }

    public static Integer decodeInt(byte[] bytes) {
        return decodeInt(bytes, 0);
    }

    public static Integer decodeInt(byte[] bytes, int index) {
        if (bytes == null) {
            return null;
        }
        return bytes[index + 3] & 0xFF | (bytes[index + 2] & 0xFF) << 8
            | (bytes[index + 1] & 0xFF) << 16 | (bytes[index] & 0xFF) << 24;
    }

    public static Long decodeLong(byte[] bytes) {
        return decodeLong(bytes, 0);
    }

    public static Long decodeLong(byte[] bytes, int index) {
        if (bytes == null) {
            return null;
        }
        return bytes[index + 7] & 0xFFL
            | (bytes[index + 6] & 0xFFL) << 8
            | (bytes[index + 5] & 0xFFL) << 16
            | (bytes[index + 4] & 0xFFL) << 24
            | (bytes[index + 3] & 0xFFL) << 32
            | (bytes[index + 2] & 0xFFL) << 40
            | (bytes[index + 1] & 0xFFL) << 48
            | (bytes[index] & 0xFFL) << 56;
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
        return new Pair(Arrays.copyOfRange(bytes, offset + lenOffset, totalOffset), lenOffset + len);
    }
}
