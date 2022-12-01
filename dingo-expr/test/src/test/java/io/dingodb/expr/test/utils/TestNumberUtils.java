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

package io.dingodb.expr.test.utils;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;

import static io.dingodb.expr.runtime.utils.NumberUtils.posMod;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNumberUtils {
    @ParameterizedTest
    @CsvSource({
        " 4,  3,  1",
        "-4,  3, -1",
        " 4, -3,  1",
        "-4, -3, -1",
    })
    public void testJavaMod(int dividend, int divisor, int result) {
        assertThat(dividend % divisor).isEqualTo(result);
    }

    @ParameterizedTest
    @CsvSource({
        " 4,  3,  1",
        "-4,  3, -1",
        " 4, -3,  1",
        "-4, -3, -1",
    })
    public void testBigDecimalRemainder(
        @NonNull BigDecimal dividend,
        BigDecimal divisor,
        BigDecimal result
    ) {
        assertThat(dividend.remainder(divisor)).isEqualTo(result);
    }

    @ParameterizedTest
    @CsvSource({
        " 4,  3,  1",
        "-4,  3,  2",
        " 4, -3,  1",
        "-4, -3,  2",
    })
    public void testPosModInt(int dividend, int divisor, int result) {
        assertThat(posMod(dividend, divisor)).isEqualTo(result);
    }

    @ParameterizedTest
    @CsvSource({
        " 4,  3,  1",
        "-4,  3,  2",
        " 4, -3,  1",
        "-4, -3,  2",
    })
    public void testPosModLong(long dividend, long divisor, long result) {
        assertThat(posMod(dividend, divisor)).isEqualTo(result);
    }
}
