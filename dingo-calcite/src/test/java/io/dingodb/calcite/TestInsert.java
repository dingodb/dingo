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
import io.dingodb.calcite.rel.DingoTableModify;
import io.dingodb.calcite.rel.DingoUnion;
import io.dingodb.calcite.rel.DingoValues;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.rel.dingo.DingoRoot;
import io.dingodb.calcite.rel.dingo.DingoScanWithRelOp;
import io.dingodb.calcite.rel.dingo.DingoStreamingConverter;
import io.dingodb.calcite.rel.logical.LogicalDingoRoot;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.expr.runtime.exception.CastingException;
import io.dingodb.test.asserts.Assert;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.util.NlsString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Slf4j
public class TestInsert {
    private static DingoParserContext context;
    private DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        MockMetaServiceProvider.init();
        Properties properties = new Properties();
        context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME, properties);
    }

    @BeforeEach
    public void setup() {
        // Create each time to clean the statistic info.
        parser = new DingoParser(context);
    }

    @Test
    public void testInsertValues() throws SqlParseException {
        String sql = "insert into test values(1, 'Alice', 1.0)";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalValues logicalValues = (LogicalValues) Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalTableModify.class).prop("operation", TableModify.Operation.INSERT)
            .soleInput().isA(LogicalValues.class)
            .getInstance();
        List<? extends List<RexLiteral>> tuples = logicalValues.getTuples();
        assertThat(tuples).size().isEqualTo(1);
        List<RexLiteral> tuple = tuples.get(0);
        assertThat(tuple).size().isEqualTo(3);
        log.info("tuple = {}", tuple);
        assertThat(tuple).element(0)
            .hasFieldOrPropertyWithValue("value", BigDecimal.valueOf(1));
        assertThat(tuple).element(1)
            .hasFieldOrPropertyWithValue("value", new NlsString(
                "Alice",
                StandardCharsets.UTF_8.name(),
                new SqlCollation(SqlCollation.Coercibility.IMPLICIT)
            ));
        assertThat(tuple).element(2)
            .hasFieldOrPropertyWithValue("value", BigDecimal.valueOf(1.0));
        // To physical plan.
        RelNode optimized = parser.optimize(relRoot.rel);
        DingoValues values = (DingoValues) Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableModify.class).prop("operation", TableModify.Operation.INSERT)
            .soleInput().isA(DingoStreamingConverter.class)
            .soleInput().isA(DingoValues.class)
            .getInstance();
        assertThat(values.getTuples()).hasSize(1).containsExactlyInAnyOrder(
            new Object[]{1, "Alice", 1.0}
        );
    }

    @Test
    public void testInsertValues1() throws SqlParseException {
        String sql = "insert into test values(1, 'Alice', 1.0), (2, 'Betty', 1.0 + 1.0)";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalTableModify.class).prop("operation", TableModify.Operation.INSERT)
            .soleInput().isA(LogicalUnion.class)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableModify.class).prop("operation", TableModify.Operation.INSERT)
            .soleInput().isA(DingoStreamingConverter.class)
            .soleInput().isA(DingoUnion.class);
    }

    @Test
    public void testInsertDateValues() throws Exception {
        String sql = "insert into `table-with-date` values(1, 'Peso', '1970-1-1'), (2,'Alice','1970-1-2')";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalTableModify.class).prop("operation", TableModify.Operation.INSERT)
            .soleInput().isA(LogicalUnion.class)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableModify.class).prop("operation", TableModify.Operation.INSERT)
            .soleInput().isA(DingoStreamingConverter.class)
            .soleInput().isA(DingoUnion.class);
    }

    @Test
    public void testInsertSelect() throws SqlParseException {
        String sql = "insert into test1 select id as id1, id as id2, id as id3, name, amount from test";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalTableModify.class).prop("operation", TableModify.Operation.INSERT)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableModify.class).prop("operation", TableModify.Operation.INSERT)
            .soleInput().isA(DingoStreamingConverter.class)
            .soleInput().isA(DingoScanWithRelOp.class);
    }

    @Test
    public void testInsertWithDefaultValue() throws Exception {
        String sql = "insert into test(id) values (1)";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalTableModify.class).prop("operation", TableModify.Operation.INSERT)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalValues.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableModify.class).prop("operation", TableModify.Operation.INSERT)
            .soleInput().isA(DingoStreamingConverter.class)
            .soleInput().isA(DingoValues.class);
    }

    @Test
    public void testInsertArrayValue() throws SqlParseException {
        String sql = "insert into `table-with-array` values (1, multiset[1, 2], array[3, 4])";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalTableModify.class).prop("operation", TableModify.Operation.INSERT)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(Collect.class)
            .soleInput().isA(LogicalValues.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableModify.class).prop("operation", TableModify.Operation.INSERT)
            .soleInput().isA(DingoStreamingConverter.class)
            .soleInput().isA(DingoValues.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        // TODO: LogicalProject(ID=[CAST(2147483648:BIGINT):INTEGER NOT NULL], NAME=['WrongId'], AMOUNT=[1.0:DOUBLE])
        "insert into `test` values(2147483648, 'WrongId', 1.0)",
        "insert into `table-with-array` values (1, multiset[1, 2], array[2147483648, 4])",
        "insert into `table-with-array` values (1, multiset[-2147483649, 2], array[3, 4])"
    })
    public void testInsertIntExceedsLimits(String sql) throws SqlParseException {
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            parser.optimize(relRoot.rel);
        });
        assertThat(exception.getCause()).isInstanceOf(CastingException.class)
            .hasMessageContaining("exceeds limits of INT");
    }
}
