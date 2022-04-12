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

import java.sql.SQLException;
import java.text.ParseException;
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
    private static Stream<Arguments> getParameters() throws ParseException {
        return Stream.of(
            arguments("1 + 2", "1 + 2"),
            arguments("1 + 2*3", "1 + 2*3"),
            arguments("1*(2 + 3)", "1*(2 + 3)"),
            arguments("name = 'Alice'", "$[1] == 'Alice'"),
            arguments("name = 'Alice' and amount > 2.0", "AND($[1] == 'Alice', $[2] > 2.0)")
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
    public void testSubStringCase01() {
        String inputStr = "DingoDatabase";
        String sql = "select substring('" + inputStr + "',1,5)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            String realResult = (String) expr.compileIn(null).eval(null);
            Assert.assrt(realResult.equals(inputStr.substring(0, 5)));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testSubStringCase02() {
        String inputStr = "DingoDatabase";
        String sql = "select substring('" + inputStr + "',1, 100)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            String realResult = (String) expr.compileIn(null).eval(null);
            Assert.assrt(realResult.equals(inputStr));
        } catch (Exception ex) {
            System.out.println("Catch Exception:" + ex.toString());
        }
    }

    @Test
    public void testSubStringCase03() {
        String inputStr = "DingoDatabase";
        String sql = "select substring('" + inputStr + "', 2, 2.5)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            String realResult = (String) expr.compileIn(null).eval(null);
            Assert.assrt(realResult.equals("ing"));
        } catch (Exception ex) {
            System.out.println("Catch Exception:" + ex.toString());
        }
    }

    @Test
    public void testSubStringCase04() {
        String inputStr = "DingoDatabase";
        String sql = "select substring('" + inputStr + "', 2, -3)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            String realResult = (String) expr.compileIn(null).eval(null);
            Assert.assrt(realResult.equals(""));
        } catch (Exception ex) {
            System.out.println("Catch Exception:" + ex.toString());
        }
    }

    @Test
    public void testSubStringCase05() {
        String inputStr = "DingoDatabase";
        String sql = "select substring('" + inputStr + "', -4, 4)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            String realResult = (String) expr.compileIn(null).eval(null);
            Assert.assrt(realResult.equals("base"));
        } catch (Exception ex) {
            System.out.println("Catch Exception:" + ex.toString());
        }
    }

    @Test
    public void testSubStringCase06() {
        String inputStr = "abcde";
        String sql = "select substring('" + inputStr + "', 1, 6)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            String realResult = (String) expr.compileIn(null).eval(null);
            Assert.assrt(realResult.equals("abcde"));
        } catch (Exception ex) {
            System.out.println("Catch Exception:" + ex.toString());
        }
    }

    @Test
    public void testTrimWithBoth() {
        String inputStr = "' AAAAA  '";
        String sql = "select trim(" + inputStr + ")";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(((String) (rtExpr.eval(null))).equals(inputStr.replace('\'', ' ').trim()));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testTrimWithBothArgs() {
        String sql = "select trim(BOTH 'A' from 'ABBA')";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(((String) (rtExpr.eval(null))).equals("BB"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testTrimWithAllSpace() {
        String sql = "select trim('  ')";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(((String) (rtExpr.eval(null))).equals(""));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testTrimWithSingleChar() {
        String sql = "select trim(' A ')";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(((String) (rtExpr.eval(null))).equals("A"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testTrimWithNumber() {
        String sql = "select trim(123 from 1234123)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(((String) (rtExpr.eval(null))).equals("4"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testTrimWithLeadingArgs() {
        String sql = "select trim(LEADING 'A' from 'ABBA')";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(((String) (rtExpr.eval(null))).equals("BBA"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testTrimWithTrailingArgs() {
        String sql = "select trim(TRAILING 'A' from 'ABBA')";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(((String) (rtExpr.eval(null))).equals("ABB"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testLTrim() {
        String value = " AAAA ";
        String sql = "select LTRIM('" + value + "')";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(((String) rtExpr.eval(null)).equals("AAAA "));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testRTrim() {
        String value = " AAAA ";
        String sql = "select rtrim('" + value + "')";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(((String) rtExpr.eval(null)).equals(" AAAA"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testUCase() {
        String sql = "select ucase('aaa')";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(((String) rtExpr.eval(null)).equals("AAA"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testLeftString01() {
        String sql = "select left('ABC', 2)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            RtExpr rtExpr = expr.compileIn(null);
            System.out.println(expr.toString());
            Assert.assrt(rtExpr.eval(null).equals("AB"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testLeftString02() {
        String sql = "select left('ABCDE', 10)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            RtExpr rtExpr = expr.compileIn(null);
            System.out.println(expr.toString());
            Assert.assrt(rtExpr.eval(null).equals("ABCDE"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testLeftString03() {
        String sql = "select left('ABCDE', -3)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            RtExpr rtExpr = expr.compileIn(null);
            System.out.println(expr.toString());
            Assert.assrt(rtExpr.eval(null).equals(""));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testRightString01() {
        String sql = "select right('ABC', 1)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(rtExpr.eval(null).equals("C"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testRightString02() {
        String sql = "select right('ABC', 1.5)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(rtExpr.eval(null).equals("BC"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testReverseString() {
        String sql = "select reverse('ABC')";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(((String) rtExpr.eval(null)).equals("CBA"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testRepeatString() {
        String sql = "select repeat('ABC', 3)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(((String)rtExpr.eval(null)).equals("ABCABCABC"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testRepeatStringDecimalIndex() {
        String sql = "select repeat('ABC', 2.1)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(((String)rtExpr.eval(null)).equals("ABCABC"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testMidString() {
        String sql = "select mid('ABC', 1, 2)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(((String)rtExpr.eval(null)).equals("AB"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testMidInvalidIndex01() {
        String sql = "select mid('ABC', 10, 3)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            // String index out of range
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testMidInvalidIndex02() {
        String sql = "select mid('ABC', 2, 3)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(((String)rtExpr.eval(null)).equals("BC"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testMidNegativeIndex01() {
        String sql = "select mid('ABCDEFG', -5, 3)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(rtExpr.eval(null).equals("CDE"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testMidNegativeIndex02() {
        String sql = "select mid('ABCDEFG', 1, -5)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(rtExpr.eval(null).equals(""));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testMidDecimalIndex01() {
        String sql = "select mid('ABCDEFG', 2.5, 3)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(rtExpr.eval(null).equals("CDE"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testMidDecimalIndex02() {
        String sql = "select mid('ABCDEFG', 2, 3.5)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(rtExpr.eval(null).equals("BCDE"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testStringLocate() {
        String sql = "select locate('C', 'ABCd')";
        try {
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
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testStringLocate01() {
        String sql = "select locate('\\c', 'ab\\cde')";
        try {
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
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testStringLocate02() {
        String sql = "select locate('', '')";
        try {
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
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testStringLocate03() {
        String sql = "select locate('abc', '')";
        try {
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
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testStringConcat() {
        String sql = "select 'AA' || 'BB' || 'CC' ";
        // String sql = "select concat('AA', 'BB', 'CC') ";
        try {
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
            Assert.assrt(((String) rtExpr.eval(null)).equals("AABBCC"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testNumberFormat01() {
        String sql = "select format(100.21, 1)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(rtExpr.eval(null).equals("100.2"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testNumberFormat02() {
        String sql = "select format(99.00000, 2)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(rtExpr.eval(null).equals("99.00"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testNumberFormat03() {
        String sql = "select format(1220.532, 0)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(rtExpr.eval(null).equals("1221"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testNumberFormat04() {
        String sql = "select format(18, 2)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(rtExpr.eval(null).equals("18.00"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testNumberFormatDecimalIndex() {
        String sql = "select format(15354.6651, 1.6)";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            RtExpr rtExpr = expr.compileIn(null);
            Assert.assrt(rtExpr.eval(null).equals("15354.67"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testNow() throws SQLException {
        String sql = "select now()";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            System.out.println(expr.toString());
            // RtExpr rtExpr = expr.compileIn(null);
            // Assert.assrt(((String)rtExpr.eval(null)).equals("100.2"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
