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
import io.dingodb.expr.runtime.RtConst;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.RtNull;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestThreeValuedLogical {
    @Nonnull
    private static Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("null and null", null),
            arguments("null and false", false),
            arguments("null and true", null),
            arguments("false and null", false),
            arguments("false and false", false),
            arguments("false and true", false),
            arguments("true and null", null),
            arguments("true and false", false),
            arguments("true and true", true),
            arguments("null or null", null),
            arguments("null or false", null),
            arguments("null or true", true),
            arguments("false or null", null),
            arguments("false or false", false),
            arguments("false or true", true),
            arguments("true or null", true),
            arguments("true or false", true),
            arguments("true or true", true),
            arguments("is_null(null)", true),
            arguments("is_null(false)", false),
            arguments("is_null(true)", false),
            arguments("is_not_null(null)", false),
            arguments("is_not_null(false)", true),
            arguments("is_not_null(true)", true),
            arguments("is_false(null)", false),
            arguments("is_false(false)", true),
            arguments("is_false(true)", false),
            arguments("is_not_false(null)", true),
            arguments("is_not_false(false)", false),
            arguments("is_not_false(true)", true),
            arguments("is_true(null)", false),
            arguments("is_true(false)", false),
            arguments("is_true(true)", true),
            arguments("is_not_true(null)", true),
            arguments("is_not_true(false)", true),
            arguments("is_not_true(true)", false),
            arguments("is_null(0)", false),
            arguments("is_not_null(0)", true),
            arguments("is_false(0)", true),
            arguments("is_not_false(0)", false),
            arguments("is_false(100)", false),
            arguments("is_not_false(100)", true),
            arguments("is_true(0)", false),
            arguments("is_not_true(0)", true),
            arguments("is_true(100)", true),
            arguments("is_not_true(100)", false)
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(String exprString, Object value) throws Exception {
        Expr expr = DingoExprCompiler.parse(exprString);
        RtExpr rtExpr = expr.compileIn(null);
        assertThat(rtExpr).isInstanceOfAny(RtConst.class, RtNull.class);
        Object result = rtExpr.eval(null);
        assertThat(result).isEqualTo(value);
    }
}
