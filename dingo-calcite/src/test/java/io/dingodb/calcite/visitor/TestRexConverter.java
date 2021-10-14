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

import io.dingodb.calcite.DingoParser;
import io.dingodb.expr.parser.Expr;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
        parser = new DingoParser(null);
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
    public void test(String sql, String result) throws SqlParseException {
        RelRoot relRoot = parser.parse("select " + sql + " from test");
        LogicalProject project = (LogicalProject) relRoot.rel;
        RexNode rexNode = project.getProjects().get(0);
        log.info("rexNode = {}", rexNode);
        Expr expr = RexConverter.convert(rexNode);
        assertThat(expr.toString()).isEqualTo(result);
    }
}
