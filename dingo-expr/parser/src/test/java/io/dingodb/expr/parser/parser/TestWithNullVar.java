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
public class TestWithNullVar {
    @RegisterExtension
    static final ContextResource res = new ContextResource(
        "/simple_vars.yml",
        "{a: null, b: 3.0, c: true, d: null}",
        "{a: 3, b: null, c: null, d: bar}"
    );

    @Nonnull
    private static Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("b", 3.0, null),
            arguments("c", true, null),
            arguments("d", null, "bar"),
            arguments("1 + a", null, 4L),
            arguments("1 + 2 * b", 7.0, null),
            arguments("$.a * $.b", null, null),
            arguments("$['a'] - $[\"b\"]", null, null),
            // short-circuit, there must be a var to prevent const optimization
            arguments("d == 'bar' || c", true, true),
            arguments("c or a = 3", true, true),
            // functions
            arguments("abs(a)", null, 3L)
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
