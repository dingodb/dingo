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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestCodecUtils {

    public static @NonNull Stream<Arguments> getVarIntArguments() {
        return Stream.of(
            arguments(0L, "00"),
            arguments(1L, "01"),
            arguments(100L, "64"),
            arguments(127L, "7F"),
            arguments(128L, "8001"),
            arguments(150L, "9601"),
            arguments(16383L, "FF7F"),
            arguments(16384L, "808001"),
            arguments(50000L, "D08603"),
            arguments(2097151L, "FFFF7F"),
            arguments(2097152L, "80808001"),
            arguments(5000000L, "C096B102"),
            arguments(268435455L, "FFFFFF7F"),
            arguments(268435456L, "8080808001"),
            arguments(500000000L, "80CAB5EE01"),
            arguments((long) Integer.MAX_VALUE, "FFFFFFFF07"),
            arguments(Long.MAX_VALUE, "FFFFFFFFFFFFFFFF7F"),
            arguments(Long.MIN_VALUE, "80808080808080808001")
        );
    }

    public static @NonNull Stream<Arguments> getDoubleArguments() {
        return Stream.of(
            arguments(7.8, "401F333333333333"),
            arguments(3.1415926, "400921FB4D12D84A"),
            arguments(3E8, "41B1E1A300000000")
        );
    }

    @ParameterizedTest(name = "Encode [{index}] [{arguments}]")
    @MethodSource("getVarIntArguments")
    public void testEncodeVarInt(long value, String result) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        CodecUtils.encodeVarInt(bos, value);
        assertThat(bos.toByteArray()).isEqualTo(CodecUtils.hexStringToBytes(result));
    }

    @ParameterizedTest(name = "Decode [{index}] [{arguments}]")
    @MethodSource("getVarIntArguments")
    public void testDecodeVarInt(long value, String source) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(CodecUtils.hexStringToBytes(source));
        long v = CodecUtils.decodeVarInt(bis);
        assertThat(v).isEqualTo(value);
    }

    @Test
    public void testDecodeVarIntException() {
        assertThrows(IOException.class, () -> {
            ByteArrayInputStream bis = new ByteArrayInputStream(new byte[]{(byte) 0xF0});
            CodecUtils.decodeVarInt(bis);
        });
    }

    @ParameterizedTest(name = "Encode [{index}] [{arguments}]")
    @MethodSource("getDoubleArguments")
    public void testEncodeDouble(double value, String result) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        CodecUtils.encodeDouble(bos, value);
        assertThat(bos.toByteArray()).isEqualTo(CodecUtils.hexStringToBytes(result));
    }
}
