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
import io.dingodb.expr.runtime.expr.Expr;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Properties;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Slf4j
public class TestRexWithTable {
    private static DingoParserContext context;

    private DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        MockMetaServiceProvider.init();
        Properties properties = new Properties();
        context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME, properties);
    }

    @Nonnull
    private static Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("name = 'Alice'", "_[1] == 'Alice'"),
            arguments("name = 'Alice' and amount > 2.0", "AND(_[1] == 'Alice', _[2] > 2.0)"),
            arguments(
                "name = 'Betty' and name = 'Alice' and amount < 1.0",
                "AND(_[1] == 'Betty', _[1] == 'Alice', _[2] < 1.0)"
            )
        );
    }

    @BeforeEach
    public void setup() {
        // Create each time to clean the statistic info.
        parser = new DingoParser(context);
    }

    private RexNode getRexNode(String rex) throws SqlParseException {
        SqlNode sqlNode = parser.parse("select " + rex + " from " + "test");
        sqlNode = parser.getSqlValidator().validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode, false);
        LogicalProject project = (LogicalProject) relRoot.rel.getInput(0);
        return project.getProjects().get(0);
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(String rex, String result) throws SqlParseException {
        RexNode rexNode = getRexNode(rex);
        Expr expr = RexConverter.convert(rexNode);
        assertThat(expr.toString()).isEqualTo(result);
    }
}
