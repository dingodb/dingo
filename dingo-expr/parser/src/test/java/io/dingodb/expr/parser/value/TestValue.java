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

package io.dingodb.expr.parser.value;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestValue {
    private static @NonNull Stream<Arguments> getParametersParseInt() {
        return Stream.of(
            arguments("1", 1),
            arguments("123", 123),
            arguments(Integer.toString(Integer.MIN_VALUE), Integer.MIN_VALUE),
            arguments(Integer.toString(Integer.MAX_VALUE), Integer.MAX_VALUE),
            arguments(Long.toString((long) Integer.MIN_VALUE - 1), (long) Integer.MIN_VALUE - 1),
            arguments(Long.toString((long) Integer.MAX_VALUE + 1), (long) Integer.MAX_VALUE + 1),
            arguments(
                BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE).toString(),
                BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE)
            ),
            arguments(
                BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE).toString(),
                BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE)
            )
        );
    }

    private static @NonNull Stream<Arguments> getParametersParseReal() {
        return Stream.of(
            arguments("1.2", new BigDecimal("1.2")),
            arguments("6.2831", new BigDecimal("6.2831"))
        );
    }

    private static @NonNull Stream<Arguments> getParametersToString() {
        return Stream.of(
            arguments("abc", "'abc'"),
            arguments(true, "true"),
            arguments(false, "false"),
            arguments(1, "1"),
            arguments(2L, "LONG(2)"),
            arguments(BigDecimal.valueOf(3), "DECIMAL(3)"),
            arguments((long) Integer.MIN_VALUE - 1, Long.toString((long) Integer.MIN_VALUE - 1)),
            arguments((long) Integer.MAX_VALUE + 1, Long.toString((long) Integer.MAX_VALUE + 1)),
            arguments(
                BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE),
                BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE).toString()
            ),
            arguments(
                BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE),
                BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE).toString()
            ),
            arguments(3.5, "DOUBLE(3.5)"),
            arguments(BigDecimal.valueOf(4.8), "4.8")
        );
    }

    @ParameterizedTest
    @MethodSource("getParametersParseInt")
    public void testParseInt(String text, Number value) {
        assertThat(((Value<?>) Value.parseInt(text)).getValue()).isEqualTo(value);
    }

    @ParameterizedTest
    @MethodSource("getParametersParseReal")
    public void testParseReal(String text, Number value) {
        assertThat(((Value<?>) Value.parseInt(text)).getValue()).isEqualTo(value);
    }

    @ParameterizedTest
    @MethodSource("getParametersToString")
    public void testToString(@NonNull Object value, String text) {
        assertThat(Value.of(value).toString()).isEqualTo(text);
    }
}
