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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
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

        public ComparableByteArray(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public int compareTo(@NonNull ComparableByteArray other) {
            if (!ignoreLen) {
                return compare(bytes, other.bytes);
            } else {
                return compareWithoutLen(bytes, other.bytes);
            }
        }

        public static class JacksonKeySerializer extends JsonSerializer<ComparableByteArray> {
            @Override
            public void serialize(ComparableByteArray value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
                byte[] bytes = unsliced(value.bytes, 1, value.bytes.length + 1);
                bytes[0] = (byte) (value.ignoreLen ? 1 : 0);
                gen.writeFieldName(new String(bytes));
            }
        }

        public static class JacksonKeyDeserializer extends com.fasterxml.jackson.databind.KeyDeserializer {
            @Override
            public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
                byte[] bytes = key.getBytes();
                return new ComparableByteArray(slice(bytes, 1, bytes.length - 1), bytes[0] != 0);
            }
        }
    }

    public static final byte[] EMPTY_BYTES = new byte[0];
    public static final byte[] MAX_BYTES = new byte[] {(byte) 0xFF };

    public static int compare(byte[] bytes1, byte[] bytes2, boolean ignoreLen) {
        if (bytes1 == bytes2) {
            return 0;
        }
        int n = Math.min(bytes1.length, bytes2.length);
        for (int i = 0; i < n; i++) {
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

    public static int compareWithoutLen(byte[] bytes1, byte[] bytes2) {
        return compare(bytes1, bytes2, true);
    }

    public static boolean equal(byte[] bytes1, byte[] bytes2) {
        return bytes2 != null && compare(bytes1, bytes2) == 0;
    }

    public static boolean lessThan(byte[] bytes1, byte[] bytes2) {
        return compare(bytes1, bytes2) < 0;
    }

    public static boolean greatThan(byte[] bytes1, byte[] bytes2) {
        return compare(bytes1, bytes2) > 0;
    }

    public static boolean lessThanOrEqual(byte[] bytes1, byte[] bytes2) {
        return compareWithoutLen(bytes1, bytes2) <= 0;
    }

    public static boolean greatThanOrEqual(byte[] bytes1, byte[] bytes2) {
        return compareWithoutLen(bytes1, bytes2) >= 0;
    }

    public static String enCodeBytes2Base64(byte[] input) {
        return new String(Base64.getEncoder().encode(input), StandardCharsets.UTF_8);
    }

    public static byte[] deCodeBase64String2Bytes(final String input) {
        return Base64.getDecoder().decode(input);
    }

    public static byte[] increment(@NonNull byte[] input) {
        if (input == null) {
            return null;
        }
        byte[] ret = new byte[input.length];
        int carry = 1;
        for (int i = input.length - 1; i >= 0; i--) {
            if (input[i] == (byte) 0xFF && carry == 1) {
                ret[i] = (byte) 0x00;
            } else {
                ret[i] = (byte) (input[i] + carry);
                carry = 0;
            }
        }

        return (carry == 0) ? ret : input;
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
