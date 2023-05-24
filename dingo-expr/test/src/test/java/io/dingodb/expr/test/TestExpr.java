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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestExpr {
    private static final double TAU = Math.PI * 2;
    private static DingoExprCompiler compiler;

    @BeforeAll
    public static void setupAll() {
        compiler = new DingoExprCompiler();
    }

    private static @NonNull Stream<Arguments> getParameters() {
        return Stream.of(
            // value
            arguments("true", true),
            arguments("false", false),
            arguments("2", 2),
            arguments(Long.toString(Long.MIN_VALUE), Long.MIN_VALUE),
            arguments(Long.toString(Long.MAX_VALUE), Long.MAX_VALUE),
            arguments(
                BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE).toString(),
                BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE)
            ),
            arguments(Long.toString(Long.MIN_VALUE), Long.MIN_VALUE),
            arguments("3.0", new BigDecimal("3.0")),
            arguments("'foo'", "foo"),
            arguments("\"bar\"", "bar"),
            arguments("'\\\\-\\/-\\b-\\n-\\r-\\t-\\u0020'", "\\-/-\b-\n-\r-\t- "),
            arguments("\"a\\\"b\"", "a\"b"),
            arguments("'a\"b'", "a\"b"),
            // arithmetic op
            arguments("1 + 2", 3),
            arguments("1 + 2 * 3", 7),
            arguments("(1 + 2) * 3", 9),
            arguments("(1 + 2) * (5 - (3 + 4))", -6),
            arguments("3 * 1.5 + 2.34", new BigDecimal("6.84")),
            arguments("2 * -3.14e2", new BigDecimal("-6.28e2")),
            arguments("5e4 + 3e3", new BigDecimal("5.3e4")),
            arguments("1 / 100", 0),
            arguments("1.0 / 100", new BigDecimal("0.0")),
            arguments("double(1.0) / 100", 0.01),
            arguments("1 + (2 * 3-4)", 3),
            arguments("mod(4, 3)", 1),
            arguments("mod(-4, 3)", -1),
            arguments("mod(4, -3)", 1),
            arguments("mod(-4, -3)", -1),
            arguments("mod(5.0, 2.5)", BigDecimal.valueOf(0.0)),
            arguments("mod(5.1, 2.5)", BigDecimal.valueOf(0.1)),
            // relational & logical op
            arguments("3 < 4", true),
            arguments("4.0 == 4", true),
            arguments("5 != 6", true),
            arguments("1 <= 2 && 3 > 2", true),
            arguments("1 > 0.1 and 2 - 2 = 0", true),
            arguments("not (0.0 * 2 < 0 || 1 * 4 > 3 and 6 / 6 == 1)", false),
            // string op
            arguments("\"Alice\" + 'Bob'", "AliceBob"),
            // mathematical fun
            arguments("abs(-1)", 1),
            // `Math.abs` behavior
            arguments("abs(" + Integer.MIN_VALUE + ")", Integer.MIN_VALUE),
            arguments("abs(" + (Integer.MIN_VALUE - 1L) + ")", Integer.MAX_VALUE + 2L),
            arguments("abs(-2.3)", BigDecimal.valueOf(2.3)),
            arguments("abs(double(-2.3))", 2.3),
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
            arguments("log(double(1.0) / E)", -1.0),
            // string functions
            arguments("lower('HeLlO')", "hello"),
            arguments("upper('HeLlO')", "HELLO"),
            arguments("substr('DingoExpression', 0, 5)", "Dingo"),
            arguments("substr('DingoExpression', 2, 3)", "n"),
            arguments("replace('I love $name', '$name', 'Lucia')", "I love Lucia"),
            arguments("trim(' HeLlO \\n\\t')", "HeLlO"),
            // date&time
            arguments("date('1970-1-1')", new Date(0)),
            arguments("time('00:00:00')", new Time(0)),
            arguments("timestamp('1970-1-1 00:00:00')", Timestamp.valueOf("1970-01-01 00:00:00")),
            arguments("timestamp('2022-04-14 00:00:00')", Timestamp.valueOf("2022-04-14 00:00:00")),
            arguments("timestamp('20220414180215')", Timestamp.valueOf("2022-04-14 18:02:15")),
            arguments("timestamp('2022/04/14 19:02:15')", Timestamp.valueOf("2022-04-14 19:02:15")),
            arguments("timestamp('2022/04/14 19:02:15.365')", Timestamp.valueOf("2022-04-14 19:02:15.365")),
            arguments("time('10:10:00') < time('11:00:02')", true),
            arguments("date('1980-01-31') >= date('1980-02-01')", false),
            arguments("timestamp('1980-01-31 23:59:59') < timestamp('1980-02-01 00:00:00')", true),
            // casting
            arguments("int(5)", 5),
            arguments("int(long(5))", 5),
            arguments("int(5.2)", 5),
            arguments("int(5.5)", 6),
            arguments("int(double(5.5))", 6),
            arguments("int(decimal(5.2))", 5),
            arguments("int('5')", 5),
            arguments("long(int(6))", 6L),
            arguments("long(6)", 6L),
            arguments("long(6.3)", 6L),
            arguments("long(decimal(6.3))", 6L),
            arguments("long('6')", 6L),
            arguments("float(int(7))", 7.0f),
            arguments("float(long(7))", 7.0f),
            arguments("float(7.4)", 7.4f),
            arguments("float(decimal(7.4))", 7.4f),
            arguments("float('7.4')", 7.4f),
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
            // min, max
            arguments("min(3, 5)", 3),
            arguments("min(7.5, 5)", BigDecimal.valueOf(5)),
            arguments("min(3, 5.0)", BigDecimal.valueOf(3)),
            arguments("max(3, 5)", 5),
            arguments("max(7.5, 5)", BigDecimal.valueOf(7.5)),
            arguments("max(3, 5.0)", BigDecimal.valueOf(5.0)),
            // null
            arguments("null", null),
            arguments("1 + null", null),
            arguments("1 + null * 3", null),
            arguments("(1 + null) * 3", null),
            arguments("(1 + 2) * (null - (3 + 4))", null),
            arguments("3 < null", null),
            arguments("null == null", null),
            arguments("null != null", null),
            arguments("1 <= 2 or null > 2", true),
            arguments("1 < 0.1 and 2 - 2 = null", false),
            arguments("1 > 0 and 2 = null", null),
            arguments("1 < 0 or null = 3", null),
            arguments("not (0.0 * null < 0 || 1 * 4 > 3 and 6 / 6 == 1)", false),
            arguments("abs(null)", null),
            arguments("int(null)", null),
            arguments("min(null, 5)", null),
            arguments("max(3, null)", null),
            // three-valued logic
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
    public void test(String exprString, Object value) throws ExprCompileException, ExprParseException {
        ExprTestUtils.testEval(compiler, exprString, value);
    }
}
