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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestDingoDateTimeUtils {
    public static @NonNull Stream<Arguments> getTestConvertFormatParameters() {
        return Stream.of(
            arguments("%Y", "uuuu"),
            arguments("%Y-%m-%d", "uuuu'-'MM'-'dd"),
            arguments("%A%B%C", "'ABC'"),
            arguments("Year: %Y, Month: %m", "'Year: 'uuuu', Month: 'MM")
        );
    }

    @ParameterizedTest
    @MethodSource("getTestConvertFormatParameters")
    public void testConvertFormat(String mysqlFormat, String result) {
        assertThat(DingoDateTimeUtils.convertFormat(mysqlFormat)).isEqualTo(result);
    }
}
