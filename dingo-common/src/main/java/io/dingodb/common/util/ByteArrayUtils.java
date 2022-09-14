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
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

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

        @Override
        public int compareTo(ComparableByteArray other) {
            return compare(bytes, other.bytes);
        }
    }

    public static final byte[] EMPTY_BYTES = new byte[0];
    public static final byte[] MAX_BYTES = new byte[] {(byte) 0xFF };

    public static int compare(byte[] bytes1, byte[] bytes2) {
        if (Arrays.equals(bytes1, bytes2)) {
            return 0;
        }
        int n = Math.min(bytes1.length, bytes2.length);
        for (int i = 0; i < n; i++) {
            if (bytes1[i] == bytes2[i]) {
                continue;
            }
            return (bytes1[i] & 0xFF) - (bytes2[i] & 0xFF);
        }
        return bytes1.length - bytes2.length;
    }

    public static boolean equal(byte[] bytes1, byte[] bytes2) {
        return compare(bytes1, bytes2) == 0;
    }

    public static boolean lessThan(byte[] bytes1, byte[] bytes2) {
        return compare(bytes1, bytes2) < 0;
    }

    public static boolean greatThan(byte[] bytes1, byte[] bytes2) {
        return compare(bytes1, bytes2) > 0;
    }

    public static boolean lessThanOrEqual(byte[] bytes1, byte[] bytes2) {
        return compare(bytes1, bytes2) <= 0;
    }

    public static boolean greatThanOrEqual(byte[] bytes1, byte[] bytes2) {
        return compare(bytes1, bytes2) >= 0;
    }

    public static String enCodeBytes2Base64(byte[] input) {
        byte[] encoded = Base64.getEncoder().encode(input);
        return new String(encoded, StandardCharsets.UTF_8);
    }

    public static byte[] deCodeBase64String2Bytes(final String input) {
        byte[] decoded = Base64.getDecoder().decode(input);
        return decoded;
    }

    public static void main(String[] args) {
        String input = "hello world";
        String encoded = enCodeBytes2Base64(input.getBytes(StandardCharsets.UTF_8));

        byte[] decodeBytes = deCodeBase64String2Bytes(encoded);
        String decodeInStr = new String(decodeBytes, StandardCharsets.UTF_8);
        System.out.println("=====> DeEncode: " + decodeInStr);
    }

}
