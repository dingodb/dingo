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
import io.dingodb.calcite.rel.DingoCoalesce;
import io.dingodb.calcite.rel.DingoDistributedValues;
import io.dingodb.calcite.rel.DingoExchange;
import io.dingodb.calcite.rel.DingoPartModify;
import io.dingodb.calcite.rel.DingoPartition;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.DingoRoot;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.test.asserts.Assert;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
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
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;

import static org.apache.calcite.config.CalciteSystemProperty.DEFAULT_CHARSET;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TestInsert {
    private static DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        DingoParserContext context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME);
        parser = new DingoParser(context);
    }

    @Test
    public void testInsertValues() throws SqlParseException {
        String sql = "insert into test values(1, 'Alice', 1.0)";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        LogicalValues logicalValues = (LogicalValues) Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .soleInput().isA(LogicalTableModify.class)
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
                DEFAULT_CHARSET.value(),
                new SqlCollation(SqlCollation.Coercibility.IMPLICIT)
            ));
        assertThat(tuple).element(2)
            .hasFieldOrPropertyWithValue("value", BigDecimal.valueOf(1.0));
        // To physical plan.
        RelNode optimized = parser.optimize(relRoot.rel);
        DingoDistributedValues values = (DingoDistributedValues) Assert.relNode(optimized).isA(DingoRoot.class)
            .soleInput().isA(DingoCoalesce.class)
            .soleInput().isA(DingoExchange.class).prop("root", true)
            .soleInput().isA(DingoPartModify.class)
            .soleInput().isA(DingoDistributedValues.class)
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
        Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .soleInput().isA(LogicalTableModify.class)
            .soleInput().isA(LogicalUnion.class)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        DingoDistributedValues values = (DingoDistributedValues) Assert.relNode(optimized).isA(DingoRoot.class)
            .soleInput().isA(DingoCoalesce.class)
            .soleInput().isA(DingoExchange.class).prop("root", true)
            .soleInput().isA(DingoPartModify.class)
            .soleInput().isA(DingoDistributedValues.class)
            .getInstance();
        assertThat(values.getTuples()).hasSize(2).containsExactlyInAnyOrder(
            new Object[]{1, "Alice", 1.0},
            new Object[]{2, "Betty", 2.0}
        );
    }

    @Test
    public void testInsertDateValues() throws Exception {
        String sql = "insert into `table-with-date` values(1, 'Peso', '1970-1-1'), (2,'Alice','1970-1-2')";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .soleInput().isA(LogicalTableModify.class)
            .soleInput().isA(LogicalUnion.class)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        DingoDistributedValues values = (DingoDistributedValues) Assert.relNode(optimized).isA(DingoRoot.class)
            .soleInput().isA(DingoCoalesce.class)
            .soleInput().isA(DingoExchange.class).prop("root", true)
            .soleInput().isA(DingoPartModify.class)
            .soleInput().isA(DingoDistributedValues.class)
            .getInstance();
        assertThat(values.getTuples()).hasSize(2).containsExactlyInAnyOrder(
            new Object[]{1, "Peso", new Date(0L)},
            new Object[]{2, "Alice", new Date(24L * 60L * 60L * 1000L)}
        );
    }

    @Test
    public void testInsertSelect() throws SqlParseException {
        String sql = "insert into test1 select id as id1, id as id2, id as id3, name, amount from test";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .soleInput().isA(LogicalTableModify.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized).isA(DingoRoot.class)
            .soleInput().isA(DingoCoalesce.class)
            .soleInput().isA(DingoExchange.class).prop("root", true)
            .soleInput().isA(DingoPartModify.class)
            .soleInput().isA(DingoExchange.class)
            .soleInput().isA(DingoPartition.class)
            .soleInput().isA(DingoProject.class)
            .soleInput().isA(DingoTableScan.class);
    }

    @Test
    public void testInsertWithDefaultValue() throws Exception {
        String sql = "insert into test(id) values (1)";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .soleInput().isA(LogicalTableModify.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalValues.class);
    }
}
