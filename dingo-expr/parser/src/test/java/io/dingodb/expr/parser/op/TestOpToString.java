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

package io.dingodb.expr.parser.op;

import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.parser.DingoExprCompiler;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.text.ParseException;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestOpToString {
    @Nonnull
    private static Stream<Arguments> getParameters() throws ParseException {
        return Stream.of(
            arguments("1 + 2", "1 + 2"),
            arguments("1 + 2*3", "1 + 2*3"),
            arguments("1*(2 + 3)", "1*(2 + 3)"),
            arguments("1 < 2 and 3 < 4 or false", "1 < 2 && 3 < 4 || false"),
            arguments("5 + a[6]*3.2 - 1.0", "5 + a[6]*3.2 - 1.0"),
            arguments("(a.b + (c + d)) * replace(f, g, h)", "(a['b'] + c + d)*replace(f, g, h)")
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(String exprString, String result) throws Exception {
        Expr expr = DingoExprCompiler.parse(exprString);
        assertThat(expr.toString()).isEqualTo(result);
    }
}
