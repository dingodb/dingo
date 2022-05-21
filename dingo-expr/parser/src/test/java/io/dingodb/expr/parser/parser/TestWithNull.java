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

import java.text.ParseException;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestWithNull {
    @Nonnull
    private static Stream<Arguments> getParameters() throws ParseException {
        return Stream.of(
            // value
            arguments("null", null),
            // arithmetic op
            arguments("1 + null", null),
            arguments("1 + null * 3", null),
            arguments("(1 + null) * 3", null),
            arguments("(1 + 2) * (null - (3 + 4))", null),
            // relational & logical op
            arguments("3 < null", null),
            arguments("null == null", null),
            arguments("null != null", null),
            arguments("1 <= 2 or null > 2", true),
            arguments("1 < 0.1 and 2 - 2 = null", false),
            arguments("1 > 0 and 2 = null", null),
            arguments("1 < 0 or null = 3", null),
            arguments("not (0.0 * null < 0 || 1 * 4 > 3 and 6 / 6 == 1)", false),
            // string op
            arguments("'abc' startsWith null", null),
            arguments("null startsWith ''", null),
            // mathematical fun
            arguments("abs(null)", null),
            // string fun
            arguments("lower(null)", null),
            arguments("substring(null, 1, 4)", null),
            // type conversion
            arguments("int(null)", null),
            // min, max
            arguments("min(null, 5)", null),
            arguments("max(3, null)", null)
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
