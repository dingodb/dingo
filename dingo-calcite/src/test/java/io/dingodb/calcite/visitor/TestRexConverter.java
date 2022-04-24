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
import io.dingodb.expr.runtime.RtExpr;
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

import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    private static Stream<Arguments> getParameters() throws ParseException {
        return Stream.of(
            arguments("1 + 2", "1 + 2"),
            arguments("1 + 2*3", "1 + 2*3"),
            arguments("1*(2 + 3)", "1*(2 + 3)"),
            arguments("name = 'Alice'", "$[1] == 'Alice'"),
            arguments("name = 'Alice' and amount > 2.0", "AND($[1] == 'Alice', $[2] > 2.0)"),
            arguments(
                "name = 'Betty' and name = 'Alice' and amount < 1.0",
                "AND($[1] == 'Betty', $[1] == 'Alice', $[2] < 1.0)"
            )
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

    @Test
    public void testSubStringCase01() throws Exception {
        String inputStr = "DingoDatabase";
        String sql = "select substring('" + inputStr + "',1,5)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        String realResult = (String) expr.compileIn(null).eval(null);
        Assert.assrt(realResult.equals(inputStr.substring(0, 5)));
    }

    @Test
    public void testSubStringCase02() throws Exception {
        String inputStr = "DingoDatabase";
        String sql = "select substring('" + inputStr + "',1, 100)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        String realResult = (String) expr.compileIn(null).eval(null);
        Assert.assrt(realResult.equals(inputStr));
    }

    @Test
    public void testSubStringCase03() throws Exception {
        String inputStr = "DingoDatabase";
        String sql = "select substring('" + inputStr + "', 2, 2.5)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        String realResult = (String) expr.compileIn(null).eval(null);
        Assert.assrt(realResult.equals("ing"));
    }

    @Test
    public void testSubStringCase04() throws Exception {
        String inputStr = "DingoDatabase";
        String sql = "select substring('" + inputStr + "', 2, -3)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        String realResult = (String) expr.compileIn(null).eval(null);
        Assert.assrt(realResult.equals(""));
    }

    @Test
    public void testSubStringCase05() throws Exception {
        String inputStr = "DingoDatabase";
        String sql = "select substring('" + inputStr + "', -4, 4)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        String realResult = (String) expr.compileIn(null).eval(null);
        Assert.assrt(realResult.equals("base"));
    }

    @Test
    public void testSubStringCase06() throws Exception {
        String inputStr = "abcde";
        String sql = "select substring('" + inputStr + "', 1, 6)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        String realResult = (String) expr.compileIn(null).eval(null);
        Assert.assrt(realResult.equals("abcde"));
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
        Assert.assrt(rtExpr.eval(null).equals("BC"));
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
        Assert.assrt(((String) rtExpr.eval(null)).equals("BC"));
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
        String sql = "select 'AA' || 'BB' || 'CC' ";
        // String sql = "select concat('AA', 'BB', 'CC') ";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());

        // Expr expr1 = DingoExprCompiler.parse("concat('A', 'B')");
        // System.out.println(expr1.toString());

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
        Assert.assrt(rtExpr.eval(null).equals("100.2"));
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
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        String result = ((java.sql.Timestamp)(rtExpr.eval(null))).toString();
        System.out.println(result);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        LocalDateTime localDateTime = LocalDateTime.parse(result, formatter);
        Long millis = System.currentTimeMillis();
        millis += TimeZone.getDefault().getRawOffset();
        LocalDateTime nowTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
        Assert.assrt(localDateTime.getYear() == nowTime.getYear());
        Assert.assrt(localDateTime.getMonth() == nowTime.getMonth());
        Assert.assrt(localDateTime.getDayOfMonth() == nowTime.getDayOfMonth());
        Assert.assrt(localDateTime.getHour() == nowTime.getHour());
        // Assert.assrt(localDateTime.getMinute() == nowTime.getMinute());
    }

    @Test
    public void formatUnixTime() throws Exception {
        String sql = "select from_unixtime(1649838034)";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        // RtExpr rtExpr = expr.compileIn(null);
        // Assert.assrt(rtExpr.eval(null).equals("2022-04-13 16:20:34"));
    }

    @Test
    public void unixTimestamp01() throws Exception {
        String sql = "select unix_timestamp('20220414')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(String.valueOf(rtExpr.eval(null)).equals("1649865600"));
    }

    @Test
    public void unixTimestamp02() throws Exception {
        String sql = "select unix_timestamp('2022-04-14')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(String.valueOf(rtExpr.eval(null)).equals("1649865600"));
    }

    @Test
    public void unixTimestamp03() throws Exception {
        String sql = "select unix_timestamp('20220414180215')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(String.valueOf(rtExpr.eval(null)).equals("1649930535"));
    }

    @Test
    public void unixTimestamp04() throws Exception {
        String sql = "select unix_timestamp('2022/04/14 18:02:15')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(String.valueOf(rtExpr.eval(null)).equals("1649930535"));
    }

    @Test
    public void unixTimestamp05() throws Exception {
        String sql = "select unix_timestamp('2022.04.15 16:27:50')";
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        Expr expr = RexConverter.convert(rexNode);
        System.out.println(expr.toString());
        RtExpr rtExpr = expr.compileIn(null);
        Assert.assrt(String.valueOf(rtExpr.eval(null)).equals("1650011270"));
    }
}
