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
    public static @NonNull Stream<Arguments> getArguments() {
        return Stream.of(
            arguments(0, new byte[]{0}),
            arguments(1, new byte[]{1}),
            arguments(100, new byte[]{100}),
            arguments(127, new byte[]{127}),
            arguments(128, new byte[]{(byte) 0x80, 0x01}),
            arguments(150, new byte[]{(byte) 0x96, 0x01}),
            arguments(16383, new byte[]{(byte) 0xFF, 0x7F}),
            arguments(16384, new byte[]{(byte) 0x80, (byte) 0x80, 0x01}),
            arguments(50000, new byte[]{(byte) 0xD0, (byte) 0x86, 0x03}),
            arguments(2097151, new byte[]{(byte) 0xFF, (byte) 0xFF, 0x7F}),
            arguments(2097152, new byte[]{(byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01}),
            arguments(5000000, new byte[]{(byte) 0xC0, (byte) 0x96, (byte) 0xB1, 0x02}),
            arguments(268435455, new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F}),
            arguments(268435456, new byte[]{(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01}),
            arguments(500000000, new byte[]{(byte) 0x80, (byte) 0xCA, (byte) 0xB5, (byte) 0xEE, 0x01}),
            arguments(Integer.MAX_VALUE, new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x07}),
            arguments(Integer.MIN_VALUE, new byte[]{(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x08})
        );
    }

    @ParameterizedTest(name = "Encode [{index}] [{arguments}]")
    @MethodSource("getArguments")
    public void testEncode(int value, byte[] result) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        CodecUtils.encodeVarInt(bos, value);
        assertThat(bos.toByteArray()).isEqualTo(result);
    }

    @ParameterizedTest(name = "Decode [{index}] [{arguments}]")
    @MethodSource("getArguments")
    public void testDecode(int value, byte[] source) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(source);
        int v = CodecUtils.decodeVarInt(bis);
        assertThat(v).isEqualTo(value);
    }

    @Test
    public void testDecodeException() {
        assertThrows(IOException.class, () -> {
            ByteArrayInputStream bis = new ByteArrayInputStream(new byte[]{(byte) 0xF0});
            CodecUtils.decodeVarInt(bis);
        });
    }
}
