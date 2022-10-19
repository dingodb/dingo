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

package io.dingodb.expr.parser.parser;

import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.runtime.RtExpr;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestIndexOp {
    @RegisterExtension
    static final ContextResource res = new ContextResource(
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

    private static @NonNull Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("arrA[0]", 1L, 4L),
            arguments("arrB[1]", "bar", "b"),
            arguments("arrA[0] + arrA[1]", 3L, 9L),
            arguments("arrB[0] + arrB[1]", "foobar", "ab"),
            arguments("arrC[0]", 1L, "def"),
            arguments("arrD[0]", 10L, 20L),
            arguments("arrD[1]", "tuple", "TUPLE"),
            arguments("mapA.a", 1L, "def"),
            arguments("mapB.foo", 2.5, 3.4),
            arguments("mapB['bar']", "TOM", "JERRY")
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(String exprString, Object value0, Object value1) throws Exception {
        Expr expr = DingoExprCompiler.parse(exprString);
        RtExpr rtExpr = expr.compileIn(res.getCtx());
        assertThat(rtExpr.eval(res.getEtx(0))).isEqualTo(value0);
        assertThat(rtExpr.eval(res.getEtx(1))).isEqualTo(value1);
    }
}
