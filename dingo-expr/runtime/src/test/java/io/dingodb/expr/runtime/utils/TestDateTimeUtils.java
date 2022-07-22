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

package io.dingodb.expr.runtime.utils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestDateTimeUtils {
    @Nonnull
    public static Stream<Arguments> getTestConvertFormatParameters() {
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
        assertThat(DateTimeUtils.convertFormat(mysqlFormat)).isEqualTo(result);
    }
}
