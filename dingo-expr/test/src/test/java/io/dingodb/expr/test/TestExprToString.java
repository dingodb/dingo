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

package io.dingodb.expr.test;

import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.parser.parser.DingoExprCompiler;
import org.assertj.core.api.Assertions;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestExprToString {
    private static DingoExprCompiler compiler;

    @BeforeAll
    public static void setupAll() {
        compiler = new DingoExprCompiler();
    }

    private static @NonNull Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("1 + 2", "1 + 2"),
            arguments("1 + 2*3", "1 + 2*3"),
            arguments("1*(2 + 3)", "1*(2 + 3)"),
            arguments("1 < 2 and 3 < 4 or false", "1 < 2 && 3 < 4 || false"),
            arguments("5 + a[6]*3.2 - 1.0", "5 + a[6]*3.2 - 1.0"),
            arguments("(a.b + (c + d)) * max(f, g)", "(a['b'] + c + d)*max(f, g)")
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(String exprString, String result) throws ExprParseException {
        Expr expr = compiler.parse(exprString);
        Assertions.assertThat(expr.toString()).isEqualTo(result);
    }
}
