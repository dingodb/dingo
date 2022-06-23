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
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestWithoutVar {
    private static final double TAU = Math.PI * 2;

    @Nonnull
    private static Stream<Arguments> getParameters() throws ParseException {
        return Stream.of(
            // value
            arguments("true", true),
            arguments("false", false),
            arguments("2", 2L),
            arguments("3.0", 3.0),
            arguments("'foo'", "foo"),
            arguments("\"bar\"", "bar"),
            arguments("'\\\\-\\/-\\b-\\n-\\r-\\t-\\u0020'", "\\-/-\b-\n-\r-\t- "),
            arguments("\"a\\\"b\"", "a\"b"),
            arguments("'a\"b'", "a\"b"),
            // arithmetic op
            arguments("1 + 2", 3L),
            arguments("1 + 2 * 3", 7L),
            arguments("(1 + 2) * 3", 9L),
            arguments("(1 + 2) * (5 - (3 + 4))", -6L),
            arguments("3 * 1.5 + 2.34", 6.84),
            arguments("2 * -3.14e2", -6.28e2),
            arguments("5e4 + 3e3", 53e3),
            arguments("1 / 100", 0L),
            arguments("1.0 / 100", 1e-2),
            arguments("1 + (2 * 3-4)", 3L),
            // relational & logical op
            arguments("3 < 4", true),
            arguments("4.0 == 4", true),
            arguments("5 != 6", true),
            arguments("1 <= 2 && 3 > 2", true),
            arguments("1 > 0.1 and 2 - 2 = 0", true),
            arguments("not (0.0 * 2 < 0 || 1 * 4 > 3 and 6 / 6 == 1)", false),
            // string op
            arguments("'abc' startsWith 'a'", true),
            arguments("'abc' startsWith 'c'", false),
            arguments("'abc' endsWith 'c'", true),
            arguments("'abc' endsWith 'b'", false),
            arguments("'abc' contains 'b'", true),
            arguments("'abc123' matches '\\\\w{3}\\\\d{3}'", true),
            arguments("'abc123' matches '.{5}'", false),
            arguments("\"Alice\" + 'Bob'", "AliceBob"),
            // mathematical fun
            arguments("abs(-1)", 1L),
            arguments("abs(-2.3)", 2.3),
            arguments("sin(0)", 0.0),
            arguments("sin(TAU / 12)", 0.5),
            arguments("sin(TAU / 4)", 1.0),
            arguments("sin(5 * TAU / 12)", 0.5),
            arguments("sin(TAU / 2)", 0.0),
            arguments("sin(7 * TAU / 12)", -0.5),
            arguments("sin(3 * TAU / 4)", -1.0),
            arguments("sin(11 * TAU / 12)", -0.5),
            arguments("sin(TAU)", 0.0),
            arguments("cos(0)", 1.0),
            arguments("cos(TAU / 6)", 0.5),
            arguments("cos(TAU / 4)", 0.0),
            arguments("cos(TAU / 3)", -0.5),
            arguments("cos(TAU / 2)", -1.0),
            arguments("cos(2 * TAU / 3)", -0.5),
            arguments("cos(3 * TAU / 4)", 0.0),
            arguments("cos(5 * TAU / 6)", 0.5),
            arguments("cos(TAU)", 1.0),
            arguments("tan(0)", 0.0),
            arguments("tan(TAU / 8)", 1.0),
            arguments("tan(3 * TAU / 8)", -1.0),
            arguments("tan(TAU / 2)", 0.0),
            arguments("tan(5 * TAU / 8)", 1.0),
            arguments("tan(7 * TAU / 8)", -1.0),
            arguments("tan(TAU)", 0.0),
            arguments("asin(-1)", -TAU / 4),
            arguments("asin(-0.5)", -TAU / 12),
            arguments("asin(0)", 0.0),
            arguments("asin(0.5)", TAU / 12),
            arguments("asin(1)", TAU / 4),
            arguments("acos(-1)", TAU / 2),
            arguments("acos(-0.5)", TAU / 3),
            arguments("acos(0)", TAU / 4),
            arguments("acos(0.5)", TAU / 6),
            arguments("acos(1)", 0.0),
            arguments("atan(-1)", -TAU / 8),
            arguments("atan(0)", 0.0),
            arguments("atan(1)", TAU / 8),
            arguments("sinh(0)", 0.0),
            arguments("cosh(0)", 1.0),
            arguments("tanh(0)", 0.0),
            arguments("cosh(2.5) + sinh(2.5)", Math.exp(2.5)),
            arguments("cosh(3.5) - sinh(3.5)", Math.exp(-3.5)),
            arguments("exp(0)", 1.0),
            arguments("exp(1)", Math.exp(1.0)),
            arguments("log(E)", 1.0),
            arguments("log(1.0 / E)", -1.0),
            // string fun
            arguments("lower('HeLlO')", "hello"),
            arguments("upper('HeLlO')", "HELLO"),
            arguments("trim(' HeLlO \\n\\t')", "HeLlO"),
            arguments("replace('I love $name', '$name', 'Lucia')", "I love Lucia"),
            // time
            arguments("time(1609300025000)", new Date(1609300025000L)),
            arguments(
                "timestamp('2020-02-20 00:00:20')",
                new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    .parse("2020-02-20 00:00:20").getTime())
            ),
            arguments(
                "date('1999-09-19', 'yyyy-MM-dd')",
                new SimpleDateFormat("yyyy-MM-dd")
                    .parse("1999-09-19")
            ),
            arguments("time(1609300025000) = time(1609300025000)", true),
            arguments("time(1609300025000) < time(1609300025001)", true),
            arguments("time(1609300025000) <= time(1609300025001)", true),
            arguments("time(1609300025000) >= time(1609300025001)", false),
            arguments("time(1609300025000) > time(1609300025001)", false),
            arguments("time(1609300025000) != time(1609300025001)", true),
            arguments("long(time(1609300025003))", 1609300025003L),
            arguments("long(time('1970-01-01+0000', 'yyyy-MM-ddZ'))", 0L),
            // type conversion
            arguments("int(5)", 5),
            arguments("int(long(5))", 5),
            arguments("int(5.2)", 5),
            arguments("int(decimal(5.2))", 5),
            arguments("int('5')", 5),
            arguments("long(int(6))", 6L),
            arguments("long(6)", 6L),
            arguments("long(6.3)", 6L),
            arguments("long(decimal(6.3))", 6L),
            arguments("long('6')", 6L),
            arguments("double(int(7))", 7.0),
            arguments("double(long(7))", 7.0),
            arguments("double(7.4)", 7.4),
            arguments("double(decimal(7.4))", 7.4),
            arguments("double('7.4')", 7.4),
            arguments("decimal(int(8))", BigDecimal.valueOf(8)),
            arguments("decimal(long(8))", BigDecimal.valueOf(8)),
            arguments("decimal(8.5)", BigDecimal.valueOf(8.5)),
            arguments("decimal(decimal(8.5))", BigDecimal.valueOf(8.5)),
            arguments("decimal('8.5')", new BigDecimal("8.5")),
            arguments("substring(string(TAU), 1, 4)", "6.28"),
            // min, max
            arguments("min(3, 5)", 3L),
            arguments("min(7.5, 5)", 5.0),
            arguments("min(decimal(3), 5.0)", BigDecimal.valueOf(3)),
            arguments("max(3, 5)", 5L),
            arguments("max(7.5, 5)", 7.5),
            arguments("max(decimal(3), 5.0)", BigDecimal.valueOf(5.0))
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(String exprString, Object value) throws Exception {
        Expr expr = DingoExprCompiler.parse(exprString);
        RtExpr rtExpr = expr.compileIn(null);
        assertThat(rtExpr).isInstanceOf(RtConst.class);
        Object result = rtExpr.eval(null);
        if (result instanceof Double) {
            assertThat((Double) result).isCloseTo((Double) value, offset(1e-6));
        } else if (result instanceof Date) {
            if (value instanceof Date) {
                assertThat(((Date) result).toLocalDate())
                    .isEqualTo(((Date) value).toLocalDate());
            } else {
                assertThat(((Date) result).toLocalDate())
                    .isEqualTo(new Date(((java.util.Date)value).getTime()).toLocalDate());
            }

        } else if (result instanceof Time) {
            assertThat(result.toString()).isEqualTo(value);
        } else {
            assertThat(result).isEqualTo(value);
        }
    }
}
