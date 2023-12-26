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

package io.dingodb.common.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.Base64Variant;
import com.fasterxml.jackson.core.Base64Variants;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ByteArrayUtils {

    private ByteArrayUtils() {
    }

    @Getter
    @Setter
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class ComparableByteArray implements Comparable<ComparableByteArray> {
        @JsonProperty
        private byte[] bytes;

        @JsonProperty
        private boolean ignoreLen = false;

        @JsonProperty
        private int pos = 0;
        public ComparableByteArray(byte[] bytes) {
            this.bytes = bytes;
        }

        public ComparableByteArray(byte[] bytes, int pos) {
            this.bytes = bytes;
            this.pos = pos;
        }

        public String encodeToString() {
            byte[] tmp = unsliced(bytes, 1, bytes.length + 1);
            tmp[0] = (byte) (ignoreLen ? 1 : 0);
            Base64Variant base64Variant = Base64Variants.getDefaultVariant();
            return base64Variant.encode(tmp);
        }

        public static ComparableByteArray decode(String key) {
            Base64Variant base64Variant = Base64Variants.getDefaultVariant();
            byte[] tmp = base64Variant.decode(key);
            return new ComparableByteArray(slice(tmp, 1, tmp.length - 1), tmp[0] != 0, 0);
        }

        @Override
        public int compareTo(@NonNull ComparableByteArray other) {
            if (!ignoreLen) {
                return compare(bytes, other.bytes, pos);
            } else {
                return compareWithoutLen(bytes, other.bytes, pos);
            }
        }

        public static class JacksonKeySerializer extends JsonSerializer<ComparableByteArray> {
            @Override
            public void serialize(ComparableByteArray value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
                gen.writeFieldName(value.encodeToString());
            }
        }

        public static class JacksonKeyDeserializer extends com.fasterxml.jackson.databind.KeyDeserializer {
            @Override
            public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
                return ComparableByteArray.decode(key);
            }
        }
    }

    public static final byte[] EMPTY_BYTES = new byte[0];

    public static final byte[] MIN = EMPTY_BYTES;
    public static final byte[] MAX = new byte[] {(byte) 0xFF };

    // todo optimized code, [namespace(1)|id(8)]
    public static final int SKIP_LONG_POS = 9;

    public static int compare(byte[] bytes1, byte[] bytes2, boolean ignoreLen) {
        return compare(bytes1, bytes2, ignoreLen, 0);
    }

    public static int compare(byte[] bytes1, byte[] bytes2, boolean ignoreLen, int pos) {
        if (bytes1 == bytes2) {
            return 0;
        }
        int n = Math.min(bytes1.length, bytes2.length);
        for (int i = pos; i < n; i++) {
            if (bytes1[i] == bytes2[i]) {
                continue;
            }
            return (bytes1[i] & 0xFF) - (bytes2[i] & 0xFF);
        }
        return ignoreLen ? 0 : bytes1.length - bytes2.length;
    }

    public static int compare(byte[] bytes1, byte[] bytes2, boolean ignoreLen, int pos, int ignoredBitsMark) {
        if (bytes1 == bytes2) {
            return 0;
        }
        int n = Math.min(bytes1.length, bytes2.length);
        for (int i = pos; i < n; i++) {
            if (i == ignoredBitsMark) {
                continue;
            }
            if (bytes1[i] == bytes2[i]) {
                continue;
            }
            return (bytes1[i] & 0xFF) - (bytes2[i] & 0xFF);
        }
        return ignoreLen ? 0 : bytes1.length - bytes2.length;
    }

    public static int compare(byte[] bytes1, byte[] bytes2) {
        return compare(bytes1, bytes2, false);
    }

    public static int compare(byte[] bytes1, byte[] bytes2, int pos) {
        return compare(bytes1, bytes2, false, pos);
    }

    public static int compare(byte[] bytes1, byte[] bytes2, int pos, int ignoredBitsMark) {
        return compare(bytes1, bytes2, false, pos, ignoredBitsMark);
    }

    public static int compareWithoutLen(byte[] bytes1, byte[] bytes2) {
        return compareWithoutLen(bytes1, bytes2, 0);
    }

    public static int compareWithoutLen(byte[] bytes1, byte[] bytes2, int pos) {
        return compare(bytes1, bytes2, true, pos);
    }

    public static boolean equal(byte[] bytes1, byte[] bytes2) {
        return bytes2 != null && compare(bytes1, bytes2) == 0;
    }

    public static boolean lessThan(byte[] bytes1, byte[] bytes2) {
        return lessThan(bytes1, bytes2, 0);
    }

    public static boolean lessThan(byte[] bytes1, byte[] bytes2, int pos) {
        return compare(bytes1, bytes2, pos) < 0;
    }

    public static boolean lessThan(byte[] bytes1, byte[] bytes2, int pos, int ignoredBitsMark) {
        return compare(bytes1, bytes2, pos, ignoredBitsMark) < 0;
    }

    public static boolean greatThan(byte[] bytes1, byte[] bytes2) {
        return greatThan(bytes1, bytes2, 0);
    }

    public static boolean greatThan(byte[] bytes1, byte[] bytes2, int pos) {
        return compare(bytes1, bytes2, false, pos) > 0;
    }

    public static boolean greatThan(byte[] bytes1, byte[] bytes2, int pos, int ignoredBitsMark) {
        return compare(bytes1, bytes2, pos, ignoredBitsMark) > 0;
    }

    public static boolean lessThanOrEqual(byte[] bytes1, byte[] bytes2) {
        return compareWithoutLen(bytes1, bytes2) <= 0;
    }

    public static boolean greatThanOrEqual(byte[] bytes1, byte[] bytes2) {
        return compareWithoutLen(bytes1, bytes2) >= 0;
    }

    public static List<byte[]> toList(byte[]... bytes) {
        return Stream.of(bytes).collect(Collectors.toList());
    }

    public static byte[] concatByteArray(byte @NonNull [] bytes1, byte @NonNull [] bytes2) {
        byte[] result = new byte[bytes1.length + bytes2.length];
        System.arraycopy(bytes1, 0, result, 0, bytes1.length);
        System.arraycopy(bytes2, 0, result, bytes1.length, bytes2.length);
        return result;
    }

    public static byte[] slice(byte @NonNull[] source, int pos, int len) {
        byte[] slice = new byte[len];
        System.arraycopy(source, pos, slice, 0, len);
        return slice;
    }

    public static byte[] unsliced(byte @NonNull[] slice, int pos) {
        return unsliced(slice, Math.max(pos, 0), Math.abs(pos) + slice.length);
    }

    public static byte[] unsliced(byte @NonNull[] slice, int pos, int len) {
        byte[] source = new byte[len];
        System.arraycopy(slice, 0, source, pos, slice.length);
        return source;
    }

}
