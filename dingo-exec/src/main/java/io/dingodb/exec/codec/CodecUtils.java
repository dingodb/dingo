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

package io.dingodb.exec.codec;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class CodecUtils {
    private CodecUtils() {
    }

    public static void encodeVarInt(OutputStream os, int value) throws IOException {
        while ((value & ~0x7F) != 0) {
            os.write((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        os.write((byte) value);
    }

    public static @NonNull Integer decodeVarInt(@NonNull InputStream is) throws IOException {
        int result = 0;
        int shift = 0;
        int b;
        while ((b = is.read()) > 0 && (b & 0x80) != 0) {
            result ^= ((b & 0x7F) << shift);
            shift += 7;
        }
        if (b < 0) {
            throw new IOException("Unexpected end of input stream.");
        }
        result |= (b << shift);
        return result;
    }
}
