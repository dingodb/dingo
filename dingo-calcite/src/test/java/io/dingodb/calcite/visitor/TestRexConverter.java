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
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.util.ExprUtil;
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.op.FunFactory;
import io.dingodb.expr.parser.op.Op;
import io.dingodb.expr.runtime.RtExpr;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.PrintWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.in;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Slf4j
public class TestRexConverter {
    private static DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        parser = new DingoParser(new DingoParserContext());
    }

    @Nonnull
    private static Stream<Arguments> getParameters() throws ParseException {
        return Stream.of(
            arguments("1 + 2", "1 + 2"),
            arguments("1 + 2*3", "1 + 2*3"),
            arguments("1*(2 + 3)", "1*(2 + 3)"),
            arguments("name = 'Alice'", "$[1] == 'Alice'"),
            arguments("name = 'Alice' and amount > 2.0", "$[1] == 'Alice' && $[2] > 2.0")
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
            Assert.assrt(realResult.equals(inputStr.substring(1, 6)));
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
            Assert.assrt(((String)(rtExpr.eval(null))).equals(inputStr.replace('\'', ' ').trim()));
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
            Assert.assrt(((String)(rtExpr.eval(null))).equals("BB"));
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
            Assert.assrt(((String)(rtExpr.eval(null))).equals("BBA"));
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
            Assert.assrt(((String)(rtExpr.eval(null))).equals("ABB"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testLTrim() {
        String sql = "select RTRIM(' AAAA ')";
        try {
            SqlNode sqlNode = parser.parse(sql);
            sqlNode = parser.validate(sqlNode);
            RelRoot relRoot = parser.convert(sqlNode);
            LogicalProject project = (LogicalProject) relRoot.rel;
            RexNode rexNode = project.getProjects().get(0);
            Expr expr = RexConverter.convert(rexNode);
            RtExpr rtExpr = expr.compileIn(null);
            System.out.println((String)rtExpr.eval(null));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
