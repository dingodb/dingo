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

package io.dingodb.calcite.visitor;

import com.ibm.icu.impl.Assert;
import io.dingodb.calcite.DingoParser;
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.mock.MockMetaServiceProvider;
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.DingoExprCompileException;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Timestamp;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Slf4j
public class TestRexConverter {
    private static DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        DingoParserContext context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME);
        parser = new DingoParser(context);
    }

    @Nonnull
    private static Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("1 + 2", "1 + 2"),
            arguments("1 + 2*3", "1 + 2*3"),
            arguments("1*(2 + 3)", "1*(2 + 3)"),
            arguments("name = 'Alice'", "$[1] == 'Alice'"),
            arguments("name = 'Alice' and amount > 2.0", "AND($[1] == 'Alice', $[2] > 2.0)"),
            arguments(
                "name = 'Betty' and name = 'Alice' and amount < 1.0",
                "AND($[1] == 'Betty', $[1] == 'Alice', $[2] < 1.0)"
            ),
            arguments("unix_timestamp('2022-04-14 00:00:00')", "unix_timestamp(timestamp('2022-04-14 00:00:00'))"),
            arguments("from_unixtime(123)", "from_unixtime(123)")
        );
    }

    @Nonnull
    private static Stream<Arguments> getEvalParameters() {
        return Stream.of(
            arguments("substring('DingoDatabase', 1, 5)", "Dingo"),
            arguments("substring('DingoDatabase', 1, 100)", "DingoDatabase"),
            arguments("substring('DingoDatabase', 2, 2.5)", "ing"),
            arguments("substring('DingoDatabase', 2, -3)", ""),
            arguments("substring('DingoDatabase', -4, 4)", "base"),
            arguments("substring('abcde', 1, 6)", "abcde")
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(String rex, String result) throws SqlParseException {
        String sql = "select " + rex + " from test";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        log.info("rexNode = {}", rexNode);
        Expr expr = RexConverter.convert(rexNode);
        assertThat(expr.toString()).isEqualTo(result);
    }

    @ParameterizedTest
    @MethodSource("getEvalParameters")
    public void testEval(String rex, String evalResult)
        throws SqlParseException, DingoExprCompileException, FailGetEvaluator {
        String sql = "select " + rex + " from test";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        log.info("rexNode = {}", rexNode);
        Expr expr = RexConverter.convert(rexNode);
        assertThat(expr.compileIn(null).eval(null)).isEqualTo(evalResult);
    }

    @Test
    public void testSubStringCase07() throws Exception {
        String inputStr = "abc";
        String sql = "select substring('" + inputStr + "', 4, 1)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        try {
            // String index out of range
            String realResult = (String) expr.compileIn(null).eval(null);
        } catch (Exception e) {
            Assert.assrt(e instanceof StringIndexOutOfBoundsException);
        }
    }

    @Test
    public void testSubStringCase08() throws Exception {
        String inputStr = "abc";
        String sql = "select substring('" + inputStr + "', 0, 1)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        try {
            // String index out of range
            String realResult = (String) expr.compileIn(null).eval(null);
        } catch (Exception e) {
            Assert.assrt(e instanceof StringIndexOutOfBoundsException);
        }
    }

    @Test
    public void testTrimWithBoth() throws Exception {
        String inputStr = "' AAAAA  '";
        String sql = "select trim(" + inputStr + ")";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals(inputStr.replace('\'', ' ').trim()));
    }

    @Test
    public void testTrimWithBothArgs() throws Exception {
        String sql = "select trim(BOTH 'A' from 'ABBA')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("BB"));
    }

    @Test
    public void testTrimWithAllSpace() throws Exception {
        String sql = "select trim('  ')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals(""));
    }

    @Test
    public void testTrimWithSingleChar() throws Exception {
        String sql = "select trim(' A ')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("A"));
    }

    @Test
    public void testTrimWithNumber() throws Exception {
        String sql = "select trim(123 from 1234123)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("4"));
    }

    @Test
    public void testTrimWithLeadingArgs() throws Exception {
        String sql = "select trim(LEADING 'A' from 'ABBA')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("BBA"));
    }

    @Test
    public void testTrimWithTrailingArgs() throws Exception {
        String sql = "select trim(TRAILING 'A' from 'ABBA')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("ABB"));
    }

    @Test
    public void testLTrim() throws Exception {
        String value = " AAAA ";
        String sql = "select LTRIM('" + value + "')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("AAAA "));
    }

    @Test
    public void testRTrim() throws Exception {
        String value = " AAAA ";
        String sql = "select rtrim('" + value + "')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals(" AAAA"));
    }

    @Test
    public void testRTrim02() throws Exception {
        String sql = "select 'a'||rtrim(' ')||'b'";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("ab"));
    }

    @Test
    public void testUCase() throws Exception {
        String sql = "select ucase('aaa')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("AAA"));
    }

    @Test
    public void testLeftString01() throws Exception {
        String sql = "select left('ABC', 2)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        RtExpr rtExpr = expr.compileIn(null);
        System.out.println(expr);
        Assert.assrt(rtExpr.eval(null).equals("AB"));
    }

    @Test
    public void testLeftString02() throws Exception {
        String sql = "select left('ABCDE', 10)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        RtExpr rtExpr = expr.compileIn(null);
        System.out.println(expr);
        Assert.assrt(rtExpr.eval(null).equals("ABCDE"));
    }

    @Test
    public void testLeftString03() throws Exception {
        String sql = "select left('ABCDE', -3)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        RtExpr rtExpr = expr.compileIn(null);
        System.out.println(expr);
        Assert.assrt(rtExpr.eval(null).equals(""));
    }

    @Test
    public void testRightString01() throws Exception {
        String sql = "select right('ABC', 1)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("C"));
    }

    @Test
    public void testRightString02() throws Exception {
        String sql = "select right('ABC', 1.5)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        assertThat(rtExpr.eval(null)).isEqualTo("BC");
    }

    @Test
    public void testReverseString() throws Exception {
        String sql = "select reverse('ABC')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("CBA"));
    }

    @Test
    public void testRepeatString() throws Exception {
        String sql = "select repeat('ABC', 3)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("ABCABCABC"));
    }

    @Test
    public void testRepeatStringDecimalIndex() throws Exception {
        String sql = "select repeat('ABC', 2.1)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("ABCABC"));
    }

    @Test
    public void testMidString01() throws Exception {
        String sql = "select mid('ABC', 1, 2)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("AB"));
    }

    @Test
    public void testMidString02() throws Exception {
        String sql = "select mid('ABC', 4, 1)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        // String index out of range
        try {
            String realResult = (String) expr.compileIn(null).eval(null);
        } catch (Exception e) {
            Assert.assrt(e instanceof StringIndexOutOfBoundsException);
        }
    }

    @Test
    public void testMidInvalidIndex01() throws Exception {
        String sql = "select mid('ABC', 10, 3)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        // String index out of range
        try {
            String realResult = (String) expr.compileIn(null).eval(null);
        } catch (Exception e) {
            Assert.assrt(e instanceof StringIndexOutOfBoundsException);
        }
    }

    @Test
    public void testMidInvalidIndex02() throws Exception {
        String sql = "select mid('ABC', 2, 3)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("BC"));
    }

    @Test
    public void testMidNegativeIndex01() throws Exception {
        String sql = "select mid('ABCDEFG', -5, 3)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("CDE"));
    }

    @Test
    public void testMidNegativeIndex02() throws Exception {
        String sql = "select mid('ABCDEFG', 1, -5)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals(""));
    }

    @Test
    public void testMidDecimalIndex01() throws Exception {
        String sql = "select mid('ABCDEFG', 2.5, 3)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("CDE"));
    }

    @Test
    public void testMidDecimalIndex02() throws Exception {
        String sql = "select mid('ABCDEFG', 2, 3.5)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("BCDE"));
    }

    @Test
    public void testMidOneParam() throws Exception {
        String sql = "select mid('ABCDEFG', 1)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("ABCDEFG"));
    }

    @Test
    public void testStringLocate() throws Exception {
        String sql = "select locate('C', 'ABCd')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        System.out.println(rtExpr.eval(null));
        Assert.assrt(Long.valueOf(rtExpr.eval(null).toString()) == 3);
    }

    @Test
    public void testStringLocate01() throws Exception {
        String sql = "select locate('\\c', 'ab\\cde')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        System.out.println(rtExpr.eval(null));
        Assert.assrt(Long.valueOf(rtExpr.eval(null).toString()) == 3);
    }

    @Test
    public void testStringLocate02() throws Exception {
        String sql = "select locate('', '')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        System.out.println(rtExpr.eval(null));
        Assert.assrt(Long.valueOf(rtExpr.eval(null).toString()) == 1);
    }

    @Test
    public void testStringLocate03() throws Exception {
        String sql = "select locate('abc', '')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        System.out.println(rtExpr.eval(null));
        Assert.assrt(Long.valueOf(rtExpr.eval(null).toString()) == 0);
    }

    @Test
    public void testStringConcat() throws Exception {
        String sql = "select concat(concat('AA', 'BB'), 'CC')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());

        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("AABBCC"));
    }

    @Test
    public void testStringConcat01() throws Exception {
        String sql = "select 'AA' || 'BB' || 'CC' ";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("AABBCC"));
    }

    @Test
    public void testNumberFormat01() throws Exception {
        String sql = "select format(100.21, 1)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        assertThat(rtExpr.eval(null)).isEqualTo("100.2");
    }

    @Test
    public void testNumberFormat02() throws Exception {
        String sql = "select format(99.00000, 2)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("99.00"));
    }

    @Test
    public void testNumberFormat03() throws Exception {
        String sql = "select format(1220.532, 0)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("1221"));
    }

    @Test
    public void testNumberFormat04() throws Exception {
        String sql = "select format(18, 2)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("18.00"));
    }

    @Test
    public void testNumberFormatDecimalIndex() throws Exception {
        String sql = "select format(15354.6651, 1.6)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(rtExpr.eval(null).equals("15354.67"));
    }

    @Test
    public void testNow() throws Exception {
        String sql = "select now()";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        assertThat((Timestamp) expr.compileIn(null).eval(null))
            .isCloseTo(DateTimeUtils.currentTimestamp(), 3L * 1000L);
    }

    @Test
    public void formatUnixTime() throws Exception {
        String dateTime = "1980-11-12 23:25:12";
        String sql = "select from_unixtime(" + Timestamp.valueOf(dateTime).getTime() / 1000L + ")";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        assertThat((Timestamp) expr.compileIn(null).eval(null))
            .isEqualTo(Timestamp.valueOf(dateTime));
    }
}
