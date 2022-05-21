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
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestThreeValuedLogicalVar {
    @RegisterExtension
    static final ContextResource res = new ContextResource(
        "/simple_vars.yml",
        "{a: 10, b: 0.0, c: true, d: null}",
        "{a: 0, b: 5.0, c: false, d: bar}"
    );

    @Nonnull
    private static Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("a and b", false, false),
            arguments("a and c", true, false),
            arguments("a and d", null, false),
            arguments("b and c", false, false),
            arguments("b and d", false, false),
            arguments("c and d", null, false),
            arguments("a or b", true, true),
            arguments("a or c", true, false),
            arguments("a or d", true, false),
            arguments("b or c", true, true),
            arguments("b or d", null, true),
            arguments("c or d", true, false),
            arguments("is_null(a)", false, false),
            arguments("is_null(b)", false, false),
            arguments("is_null(c)", false, false),
            arguments("is_null(d)", true, false),
            arguments("is_not_null(a)", true, true),
            arguments("is_not_null(b)", true, true),
            arguments("is_not_null(c)", true, true),
            arguments("is_not_null(d)", false, true),
            arguments("is_false(a)", false, true),
            arguments("is_false(b)", true, false),
            arguments("is_false(c)", false, true),
            arguments("is_false(d)", false, true),
            arguments("is_not_false(a)", true, false),
            arguments("is_not_false(b)", false, true),
            arguments("is_not_false(c)", true, false),
            arguments("is_not_false(d)", true, false),
            arguments("is_true(a)", true, false),
            arguments("is_true(b)", false, true),
            arguments("is_true(c)", true, false),
            arguments("is_true(d)", false, false),
            arguments("is_not_true(a)", false, true),
            arguments("is_not_true(b)", true, false),
            arguments("is_not_true(c)", false, true),
            arguments("is_not_true(d)", true, true)
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
