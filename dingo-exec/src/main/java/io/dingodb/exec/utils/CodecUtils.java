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

package io.dingodb.exec.utils;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class CodecUtils {
    private CodecUtils() {
    }

    public static void encodeVarInt(@NonNull OutputStream os, long value) throws IOException {
        while ((value & ~0x7F) != 0) {
            os.write((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        os.write((byte) value);
    }

    public static long decodeVarInt(@NonNull InputStream is) throws IOException {
        long result = 0;
        int shift = 0;
        long b;
        while ((b = is.read()) > 0 && (b & 0x80) != 0) {
            result |= ((b & 0x7F) << shift);
            shift += 7;
        }
        if (b < 0) {
            throw new IOException("Unexpected end of input stream.");
        }
        result |= (b << shift);
        return result;
    }

    public static void encodeFloat(@NonNull OutputStream os, float value) throws IOException {
        int bits = Float.floatToIntBits(value);
        os.write((byte) (bits >>> 24));
        os.write((byte) (bits >>> 16));
        os.write((byte) (bits >>> 8));
        os.write((byte) bits);
    }

    public static void encodeDouble(@NonNull OutputStream os, double value) throws IOException {
        long bits = Double.doubleToLongBits(value);
        os.write((byte) (bits >>> 56));
        os.write((byte) (bits >>> 48));
        os.write((byte) (bits >>> 40));
        os.write((byte) (bits >>> 32));
        os.write((byte) (bits >>> 24));
        os.write((byte) (bits >>> 16));
        os.write((byte) (bits >>> 8));
        os.write((byte) bits);
    }

    public static byte @NonNull [] hexStringToBytes(@NonNull String hex) {
        int size = hex.length();
        byte[] result = new byte[size / 2];
        for (int i = 0; i < result.length; ++i) {
            result[i] = (byte) Integer.parseInt(hex.substring(i + i, i + i + 2), 16);
        }
        return result;
    }
}
