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
public class TestIndexOp {
    @RegisterExtension
    static final ExprContext context = new ExprContext(
        "/composite_vars.yml",
        "{"
            + "arrA: [1, 2, 3],"
            + "arrB: [foo, bar],"
            + "arrC: [1, abc],"
            + "arrD: [10, tuple],"
            + "mapA: {a: 1, b: abc},"
            + "mapB: {foo: 2.5, bar: TOM}"
            + "}",
        "{"
            + "arrA: [4, 5, 6],"
            + "arrB: [a, b],"
            + "arrC: [def, 1],"
            + "arrD: [20, TUPLE],"
            + "mapA: {a: def, b: 1},"
            + "mapB: {foo: 3.4, bar: JERRY}"
            + "}"
    );

    private static DingoExprCompiler compiler;

    @BeforeAll
    public static void setupAll() {
        compiler = new DingoExprCompiler();
    }

    private static @NonNull Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("arrA[0]", 0, 1L),
            arguments("arrA[0]", 1, 4L),
            arguments("arrB[1]", 0, "bar"),
            arguments("arrB[1]", 1, "b"),
            arguments("arrA[0] + arrA[1]", 0, 3L),
            arguments("arrA[0] + arrA[1]", 1, 9L),
            arguments("arrB[0] + arrB[1]", 0, "foobar"),
            arguments("arrB[0] + arrB[1]", 1, "ab"),
            arguments("arrC[0]", 0, 1L),
            arguments("arrC[0]", 1, "def"),
            arguments("arrD[0]", 0, 10L),
            arguments("arrD[0]", 1, 20L),
            arguments("arrD[1]", 0, "tuple"),
            arguments("arrD[1]", 1, "TUPLE"),
            arguments("mapA.a", 0, 1L),
            arguments("mapA.a", 1, "def"),
            arguments("mapB.foo", 0, 2.5),
            arguments("mapB.foo", 1, 3.4),
            arguments("mapB['bar']", 0, "TOM"),
            arguments("mapB['bar']", 1, "JERRY")
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(String exprString, int index, Object value) throws ExprCompileException, ExprParseException {
        ExprTestUtils.testEvalWithVar(compiler, context, index, exprString, value);
    }
}
