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
import io.dingodb.common.exception.DingoSqlException;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import io.dingodb.test.asserts.Assert;
import io.dingodb.test.cases.RexCasesJUnit5;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Properties;
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
        Properties properties = new Properties();
        context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME, properties);
    }

    @Nonnull
    private static Stream<Arguments> getParametersExprException() {
        return Stream.of(
            arguments("concat('a', 'b', 'c')", CalciteContextException.class),
            arguments("throw()", DingoSqlException.class)
        );
    }

    @Nonnull
    private static Stream<Arguments> getParametersTemp() {
        return Stream.of(
            arguments("pow('10', -2)", "POW(DECIMAL('10'), -2)", BigDecimal.valueOf(0.01))
        );
    }

    private RexNode getRexNode(String rex) throws SqlParseException {
        SqlNode sqlNode = parser.parse("select " + rex);
        sqlNode = parser.getSqlValidator().validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode, false);
        RelNode relNode = relRoot.rel.getInput(0);
        if (relNode instanceof LogicalProject) {
            LogicalProject project = (LogicalProject) relNode;
            return project.getProjects().get(0);
        } else if (relNode instanceof LogicalValues) {
            LogicalValues values = (LogicalValues) relNode;
            return values.getTuples().get(0).get(0);
        }
        throw new IllegalArgumentException("Cannot process rex \"" + rex + "\".");
    }

    @BeforeEach
    public void setup() {
        // Create each time to clean the statistic info.
        parser = new DingoParser(context);
    }

    /**
     * Contains temporary test cases for debugging.
     *
     * @param rex      the sql expression
     * @param exprStr  the expected dingo expr string
     * @param expected the expected evaluation result
     */
    @ParameterizedTest
    @MethodSource({"getParametersTemp"})
    public void testTemp(String rex, String exprStr, Object expected)
        throws SqlParseException {
        test(rex, exprStr, expected);
    }

    @ParameterizedTest
    @ArgumentsSource(RexCasesJUnit5.class)
    public void test(String rex, String exprStr, Object expected)
        throws SqlParseException {
        RexNode rexNode = getRexNode(rex);
        Expr expr = RexConverter.convert(rexNode);
        assertThat(expr.toString()).isEqualTo(exprStr);
        Assert.of(ExprCompiler.ADVANCED.visit(expr).eval()).isEqualTo(expected);
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
        assertThat((Timestamp) ExprCompiler.ADVANCED.visit(expr).eval())
            .isCloseTo(DateTimeUtils.currentTimestamp(), 3L * 1000L);
    }

    @ParameterizedTest
    @MethodSource("getParametersExprException")
    public void testExprException(String str, Class<? extends Exception> exceptionClass) {
        assertThrows(exceptionClass, () -> {
            RexNode rexNode = getRexNode(str);
            Expr expr = RexConverter.convert(rexNode);
            ExprCompiler.ADVANCED.visit(expr).eval();
        });
    }
}
