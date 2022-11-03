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

import io.dingodb.expr.parser.exception.ExprCompileException;
import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.parser.parser.DingoExprCompiler;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestWithVar {
    @RegisterExtension
    static final ExprContext context = new ExprContext(
        "/simple_vars.yml",
        "{a: 2, b: 3.0, c: true, d: foo}",
        "{a: 3, b: 4.0, c: false, d: bar}",
        "{a: null, b: null, c: null, d: null}",
        "{a: 10, b: 0.0, c: true, d: null}",
        "{a: 0, b: 5.0, c: false, d: bar}"
    );

    private static DingoExprCompiler compiler;

    @BeforeAll
    public static void setupAll() {
        compiler = new DingoExprCompiler();
    }

    private static @NonNull Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("b", 0, 3.0),
            arguments("b", 1, 4.0),
            arguments("b", 2, null),
            arguments("c", 0, true),
            arguments("c", 1, false),
            arguments("c", 2, null),
            arguments("d", 0, "foo"),
            arguments("d", 1, "bar"),
            arguments("d", 2, null),
            arguments("1 + a", 0, 3L),
            arguments("1 + a", 1, 4L),
            arguments("1 + a", 2, null),
            arguments("1 + 2 * b", 0, 7.0),
            arguments("1 + 2 * b", 1, 9.0),
            arguments("1 + 2 * b", 2, null),
            arguments("$.a * $.b", 0, 6.0),
            arguments("$.a * $.b", 1, 12.0),
            arguments("$.a * $.b", 2, null),
            arguments("$['a'] - $[\"b\"]", 0, -1.0),
            arguments("$['a'] - $[\"b\"]", 1, -1.0),
            arguments("$['a'] - $[\"b\"]", 2, null),
            // short-circuit, there must be a var to prevent const optimization
            arguments("false and a/0", 0, false),
            arguments("false and a/0", 1, false),
            arguments("true or a/0", 0, true),
            arguments("true or a/0", 1, true),
            arguments("is_null(d) || c", 2, true),
            arguments("c && is_null(d)", 2, null),
            // functions
            arguments("abs(a)", 0, 2L),
            arguments("abs(a)", 1, 3L),
            arguments("abs(a)", 2, null),
            // three-valued logical
            arguments("d == null", 2, null),
            arguments("is_null(d)", 2, true),
            arguments("a and b != 0.0", 3, false),
            arguments("a and b", 4, false),
            arguments("a and c", 3, true),
            arguments("a and c", 4, false),
            arguments("a and d", 3, null),
            arguments("a and d", 4, false),
            arguments("b != 0.0 and c", 3, false),
            arguments("b != 0.0 and c", 4, false),
            arguments("c and d", 3, null),
            arguments("c and d", 4, false),
            arguments("a or b != 0.0", 3, true),
            arguments("a or b != 0.0", 4, true),
            arguments("a or c", 3, true),
            arguments("a or c", 4, false),
            arguments("b != 0.0 or c", 3, true),
            arguments("b != 0.0 or c", 4, true),
            arguments("is_null(a)", 3, false),
            arguments("is_null(a)", 4, false),
            arguments("is_null(b)", 3, false),
            arguments("is_null(b)", 4, false),
            arguments("is_null(c)", 3, false),
            arguments("is_null(c)", 4, false),
            arguments("is_null(d)", 3, true),
            arguments("is_null(d)", 4, false),
            arguments("is_not_null(a)", 3, true),
            arguments("is_not_null(a)", 4, true),
            arguments("is_not_null(b)", 3, true),
            arguments("is_not_null(b)", 4, true),
            arguments("is_not_null(c)", 3, true),
            arguments("is_not_null(c)", 4, true),
            arguments("is_not_null(d)", 3, false),
            arguments("is_not_null(d)", 4, true),
            arguments("is_false(a)", 3, false),
            arguments("is_false(a)", 4, true),
            arguments("is_false(b != 0.0)", 3, true),
            arguments("is_false(b != 0.0)", 4, false),
            arguments("is_false(c)", 3, false),
            arguments("is_false(c)", 4, true),
            arguments("is_not_false(a)", 3, true),
            arguments("is_not_false(a)", 4, false),
            arguments("is_not_false(b != 0.0)", 3, false),
            arguments("is_not_false(b != 0.0)", 4, true),
            arguments("is_not_false(c)", 3, true),
            arguments("is_not_false(c)", 4, false),
            arguments("is_true(a)", 3, true),
            arguments("is_true(a)", 4, false),
            arguments("is_true(b != 0.0)", 3, false),
            arguments("is_true(b != 0.0)", 4, true),
            arguments("is_true(c)", 3, true),
            arguments("is_true(c)", 4, false),
            arguments("is_not_true(a)", 3, false),
            arguments("is_not_true(a)", 4, true),
            arguments("is_not_true(b != 0.0)", 3, true),
            arguments("is_not_true(b != 0.0)", 4, false),
            arguments("is_not_true(c)", 3, false),
            arguments("is_not_true(c)", 4, true)
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(String exprString, int index, Object value) throws ExprParseException, ExprCompileException {
        ExprTestUtils.testEvalWithVar(compiler, context, index, exprString, value);
    }
}
