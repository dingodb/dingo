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

package io.dingodb.calcite;

import io.dingodb.calcite.mock.MockMetaServiceProvider;
import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.exec.utils.DingoDateTimeUtils;
import io.dingodb.expr.core.Casting;
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.ExprCompileException;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.TimeZone;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestRex {
    private static DingoParserContext context;
    private DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        MockMetaServiceProvider.init();
        context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME);
    }

    @Nonnull
    private static Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("1 + 2", "1 + 2", 3),
            arguments("1 + 2*3", "1 + 2*3", 7),
            arguments("1*(2 + 3)", "1*(2 + 3)", 5),
            // number
            arguments("abs(15354.6651)", "abs(15354.6651)", BigDecimal.valueOf(15354.6651)),
            arguments("abs(-163.4)", "abs(-163.4)", BigDecimal.valueOf(163.4)),
            arguments(
                "abs(" + (Integer.MIN_VALUE + 1) + ")",
                "abs(" + (Integer.MIN_VALUE + 1) + ")",
                Integer.MAX_VALUE
            ),
            arguments(
                "abs(" + (Integer.MIN_VALUE - 1L) + ")",
                "abs(" + (Integer.MIN_VALUE - 1L) + ")",
                Integer.MAX_VALUE + 2L
            ),
            arguments("floor(125.6)", "floor(125.6)", BigDecimal.valueOf(125)),
            arguments("floor(-125.3)", "floor(-125.3)", BigDecimal.valueOf(-126)),
            arguments("ceiling(4533.66)", "ceil(4533.66)", BigDecimal.valueOf(4534)),
            arguments("ceil(-125.3)", "ceil(-125.3)", BigDecimal.valueOf(-125)),
            arguments("format(100.21, 1)", "format(100.21, 1)", "100.2"),
            arguments("format(99.00000, 2)", "format(99.00000, 2)", "99.00"),
            arguments("format(1220.532, 0)", "format(1220.532, 0)", "1221"),
            arguments("format(18, 2)", "format(18, 2)", "18.00"),
            arguments("format(15354.6651, 1.6)", "format(15354.6651, 1.6)", "15354.67"),
            arguments("mod(99, 2)", "mod(99, 2)", BigDecimal.ONE),
            arguments("mod(-107, 2)", "mod(-107, 2)", BigDecimal.valueOf(-1)),
            arguments("pow(3, 3)", "pow(3, 3)", BigDecimal.valueOf(27)),
            arguments("pow(-3.1, 3)", "pow(-3.1, 3)", BigDecimal.valueOf(-29.791)),
            arguments("pow('10', -2)", "pow(DECIMAL('10'), -2)", BigDecimal.valueOf(0.01)),
            arguments("round(12.677, 2)", "round(12.677, 2)", BigDecimal.valueOf(12.68)),
            arguments("round(195334.12, -4)", "round(195334.12, -4)", BigDecimal.valueOf(200000)),
            arguments("round(-155.586, -2)", "round(-155.586, -2)", BigDecimal.valueOf(-200)),
            arguments("round(105, -2)", "round(105, -2)", BigDecimal.valueOf(100)),
            arguments("round(105, '-2')", "round(105, LONG('-2'))", BigDecimal.valueOf(100)),
            // string
            arguments("'AA' || 'BB' || 'CC'", "concat(concat('AA', 'BB'), 'CC')", "AABBCC"),
            arguments("concat(concat('AA', 'BB'), 'CC')", "concat(concat('AA', 'BB'), 'CC')", "AABBCC"),
            arguments("left('ABC', 2)", "left('ABC', 2)", "AB"),
            arguments("left('ABCDE', 10)", "left('ABCDE', 10)", "ABCDE"),
            arguments("left('ABCDE', -3)", "left('ABCDE', -3)", ""),
            arguments("right('ABC', 1)", "right('ABC', 1)", "C"),
            arguments("right('ABC', 1.5)", "right('ABC', 1.5)", "BC"),
            arguments("ltrim(' AAA ')", "ltrim(' AAA ')", "AAA "),
            arguments("rtrim(' AAA ')", "rtrim(' AAA ')", " AAA"),
            arguments("'a'||rtrim(' ')||'b'", "concat(concat('a', rtrim(' ')), 'b')", "ab"),
            arguments("locate('\\c', 'ab\\cde')", "locate('\\\\c', 'ab\\\\cde')", 3),
            arguments("locate('C', 'ABCd')", "locate('C', 'ABCd')", 3),
            arguments("locate('abc', '')", "locate('abc', '')", 0),
            arguments("locate('', '')", "locate('', '')", 1),
            arguments("mid('ABC', 1, 2)", "mid('ABC', 1, 2)", "AB"),
            arguments("mid('ABC', 2, 3)", "mid('ABC', 2, 3)", "BC"),
            arguments("mid('ABCDEFG', -5, 3)", "mid('ABCDEFG', -5, 3)", "CDE"),
            arguments("mid('ABCDEFG', 1, -5)", "mid('ABCDEFG', 1, -5)", ""),
            arguments("mid('ABCDEFG', 2)", "mid('ABCDEFG', 2)", "BCDEFG"),
            arguments("mid('ABCDEFG', 2.5, 3)", "mid('ABCDEFG', 2.5, 3)", "CDE"),
            arguments("mid('ABCDEFG', 2, 3.5)", "mid('ABCDEFG', 2, 3.5)", "BCDE"),
            arguments("repeat('ABC', 3)", "repeat('ABC', 3)", "ABCABCABC"),
            arguments("repeat('ABC', 2.1)", "repeat('ABC', 2.1)", "ABCABC"),
            arguments("replace('MongoDB', 'Mongo', 'Dingo')", "replace('MongoDB', 'Mongo', 'Dingo')", "DingoDB"),
            arguments("replace(null, 'a', 'b')", "replace(NULL, 'a', 'b')", null),
            arguments("replace('MongoDB', null, 'b')", "replace('MongoDB', NULL, 'b')", null),
            arguments("reverse('ABC')", "reverse('ABC')", "CBA"),
            arguments("substring('DingoDatabase', 1, 5)", "substring('DingoDatabase', 1, 5)", "Dingo"),
            arguments("substring('DingoDatabase', 1, 100)", "substring('DingoDatabase', 1, 100)", "DingoDatabase"),
            arguments("substring('DingoDatabase', 2, 2.5)", "substring('DingoDatabase', 2, 2.5)", "ing"),
            arguments("substring('DingoDatabase', 2, -3)", "substring('DingoDatabase', 2, -3)", ""),
            arguments("substring('DingoDatabase', -4, 4)", "substring('DingoDatabase', -4, 4)", "base"),
            arguments("substring('abcde', 1, 6)", "substring('abcde', 1, 6)", "abcde"),
            arguments("trim(' AAAAA  ')", "trim('BOTH', ' ', ' AAAAA  ')", "AAAAA"),
            arguments("trim(BOTH 'A' from 'ABBA')", "trim('BOTH', 'A', 'ABBA')", "BB"),
            arguments("trim('  ')", "trim('BOTH', ' ', '  ')", ""),
            arguments("trim(' A ')", "trim('BOTH', ' ', ' A ')", "A"),
            arguments("trim(123 from 1234123)", "trim('BOTH', STRING(123), STRING(1234123))", "4"),
            arguments("trim(LEADING 'A' from 'ABBA')", "trim('LEADING', 'A', 'ABBA')", "BBA"),
            arguments("trim(TRAILING 'A' from 'ABBA')", "trim('TRAILING', 'A', 'ABBA')", "ABB"),
            arguments("upper('aaa')", "upper('aaa')", "AAA"),
            arguments("ucase('aaa')", "upper('aaa')", "AAA"),
            arguments("lower('aaA')", "lower('aaA')", "aaa"),
            arguments("lcase('Aaa')", "lower('Aaa')", "aaa"),
            // date & time
            unixTimestampCase("1980-11-12 23:25:12"),
            unixTimestampCase("2022-04-14 00:00:00"),
            unixTimestampLiteralCase("1980-11-12 23:25:12"),
            unixTimestampLiteralCase("2022-04-14 00:00:00"),
            fromUnixTimeCase("1980-11-12 23:25:12"),
            fromUnixTimeCase("2022-04-14 00:00:00"),
            arguments("date_format('2022/7/2')", "date_format(DATE('2022\\/7\\/2'))", "2022-07-02"),
            arguments("time_format('235959')", "time_format(TIME('235959'))", "23:59:59"),
            arguments(
                "timestamp_format('2022/07/22 12:00:00')",
                "timestamp_format(TIMESTAMP('2022\\/07\\/22 12:00:00'))",
                "2022-07-22 12:00:00"
            ),
            timestampFormatNumericCase(1668477663, "%Y-%m-%d %H:%i:%S")
        );
    }

    private static @NonNull Arguments unixTimestampCase(@Nonnull String timestampStr) {
        Timestamp timestamp = DateTimeUtils.parseTimestamp(timestampStr);
        assert timestamp != null;
        return arguments(
            "unix_timestamp('" + timestampStr + "')",
            "unix_timestamp(TIMESTAMP('" + timestampStr + "'))",
            timestamp.getTime() / 1000L
        );
    }

    private static @NonNull Arguments unixTimestampLiteralCase(@Nonnull String timestampStr) {
        Timestamp timestamp = DateTimeUtils.parseTimestamp(timestampStr);
        assert timestamp != null;
        long millis = timestamp.getTime();
        millis = millis + TimeZone.getDefault().getOffset(millis);
        return arguments(
            "unix_timestamp(TIMESTAMP '" + timestampStr + "')",
            "unix_timestamp(TIMESTAMP(" + millis + "))",
            millis / 1000L
        );
    }

    private static @NonNull Arguments fromUnixTimeCase(@Nonnull String timestampStr) {
        Timestamp timestamp = DateTimeUtils.parseTimestamp(timestampStr);
        assert timestamp != null;
        return arguments(
            "from_unixtime(" + timestamp.getTime() / 1000L + ")",
            "from_unixtime(" + timestamp.getTime() / 1000L + ")",
            timestamp
        );
    }

    private static @NonNull Arguments timestampFormatNumericCase(@Nonnull Number timestamp, String format) {
        return arguments(
            "timestamp_format(" + timestamp + ", " + "'" + format + "')",
            "timestamp_format(" + timestamp + ", " + "'" + format + "')",
            DingoDateTimeUtils.timestampFormat(DingoDateTimeUtils.fromUnixTimestamp(
                Casting.doubleToLong(timestamp.doubleValue())
            ), format)
        );
    }

    @Nonnull
    private static Stream<Arguments> getParametersExprException() {
        return Stream.of(
            arguments("abs(" + Integer.MIN_VALUE + ")", ArithmeticException.class),
            arguments("abs(" + Long.MIN_VALUE + ")", ArithmeticException.class),
            arguments("substring('abc', 4, 1)", StringIndexOutOfBoundsException.class),
            arguments("substring('abc', 0, 1)", StringIndexOutOfBoundsException.class),
            arguments("mid('ABC', 4, 1)", StringIndexOutOfBoundsException.class),
            arguments("mid('ABC', 10, 3)", StringIndexOutOfBoundsException.class),
            arguments("concat('a', 'b', 'c')", CalciteContextException.class)
        );
    }

    @Nonnull
    private static Stream<Arguments> getParametersTemp() {
        return Stream.of(
            arguments("1 + 1", "1 + 1", 2)
        );
    }

    private RexNode getRexNode(String rex) throws SqlParseException {
        SqlNode sqlNode = parser.parse("select " + rex);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode, false);
        LogicalProject project = (LogicalProject) relRoot.rel.getInput(0);
        return project.getProjects().get(0);
    }

    @BeforeEach
    public void setup() {
        // Create each time to clean the statistic info.
        parser = new DingoParser(context);
    }

    /**
     * Contains temporary test cases for debugging.
     *
     * @param rex     the sql expression
     * @param exprStr the expected dingo expr string
     * @param result  the expected evaluation result
     */
    @ParameterizedTest
    @MethodSource({"getParametersTemp"})
    public void testTemp(String rex, String exprStr, Object result)
        throws SqlParseException, ExprCompileException {
        test(rex, exprStr, result);
    }

    @ParameterizedTest
    @MethodSource({"getParameters"})
    public void test(String rex, String exprStr, Object result)
        throws SqlParseException, ExprCompileException {
        RexNode rexNode = getRexNode(rex);
        Expr expr = RexConverter.convert(rexNode);
        assertThat(expr.toString()).isEqualTo(exprStr);
        assertThat(expr.compileIn(null).eval(null)).isEqualTo(result);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "now()",
        "current_timestamp",
        "current_timestamp()"
    })
    public void testNow(String str) throws Exception {
        RexNode rexNode = getRexNode(str);
        Expr expr = RexConverter.convert(rexNode);
        assertThat(expr.toString()).isEqualTo("current_timestamp()");
        assertThat((Timestamp) expr.compileIn(null).eval(null))
            .isCloseTo(DingoDateTimeUtils.currentTimestamp(), 3L * 1000L);
    }

    @ParameterizedTest
    @MethodSource("getParametersExprException")
    public void testExprException(String str, Class<? extends Exception> exceptionClass) {
        Exception exception = assertThrows(exceptionClass, () -> {
            RexNode rexNode = getRexNode(str);
            Expr expr = RexConverter.convert(rexNode);
            expr.compileIn(null).eval(null);
        });
    }
}
