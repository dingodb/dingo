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

import io.dingodb.calcite.assertion.Assert;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.util.NlsString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.apache.calcite.config.CalciteSystemProperty.DEFAULT_CHARSET;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TestLogicalPlan {
    private static DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        DingoParserContext context = new DingoParserContext();
        parser = new DingoParser(context);
    }

    private static RelRoot parse(String sql) throws SqlParseException {
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        log.info("relRoot = {}", relRoot);
        return relRoot;
    }

    @Test
    public void testSimple() throws SqlParseException {
        String sql = "select 1";
        RelRoot relRoot = parse(sql);
        Assert.relNode(relRoot.rel).typeName("LogicalValues").convention(Convention.NONE);
    }

    @Test
    public void testFullScan() throws SqlParseException {
        String sql = "select * from test";
        RelRoot relRoot = parse(sql);
        Assert.relNode(relRoot.rel).typeName("LogicalProject").convention(Convention.NONE)
            .singleInput().typeName("DingoTableScan").convention(DingoConventions.DINGO);
    }

    @Test
    public void testFilterScan() throws SqlParseException {
        String sql = "select * from test where name = 'Alice' and amount > 3.0";
        RelRoot relRoot = parse(sql);
        Assert.relNode(relRoot.rel).typeName("LogicalProject").convention(Convention.NONE)
            .singleInput().typeName("LogicalFilter").convention(Convention.NONE)
            .singleInput().typeName("DingoTableScan").convention(DingoConventions.DINGO);
    }

    @Test
    public void testProjectScan() throws SqlParseException {
        String sql = "select name, amount from test";
        RelRoot relRoot = parse(sql);
        Assert.relNode(relRoot.rel).typeName("LogicalProject").convention(Convention.NONE)
            .singleInput().typeName("DingoTableScan").convention(DingoConventions.DINGO);
    }

    @Test
    public void testProjectFilterScan() throws SqlParseException {
        String sql = "select name, amount from test where amount > 3.0";
        RelRoot relRoot = parse(sql);
        Assert.relNode(relRoot.rel).typeName("LogicalProject").convention(Convention.NONE)
            .singleInput().typeName("LogicalFilter").convention(Convention.NONE)
            .singleInput().typeName("DingoTableScan").convention(DingoConventions.DINGO);
    }

    @Test
    public void testAggregate() throws SqlParseException {
        String sql = "select count(*) from test";
        RelRoot relRoot = parse(sql);
        Assert.relNode(relRoot.rel).typeName("LogicalAggregate").convention(Convention.NONE)
            .singleInput().typeName("DingoTableScan").convention(DingoConventions.DINGO);
    }

    @Test
    public void testAggregateGroup() throws SqlParseException {
        String sql = "select name, sum(amount) from test group by name";
        RelRoot relRoot = parse(sql);
        Assert.relNode(relRoot.rel).typeName("LogicalAggregate").convention(Convention.NONE)
            .singleInput().typeName("LogicalProject").convention(Convention.NONE)
            .singleInput().typeName("DingoTableScan").convention(DingoConventions.DINGO);
    }

    @Test
    public void testAggregateGroup1() throws SqlParseException {
        String sql = "select sum(amount) from test group by name";
        RelRoot relRoot = parse(sql);
        Assert.relNode(relRoot.rel).typeName("LogicalProject").convention(Convention.NONE)
            .singleInput().typeName("LogicalAggregate").convention(Convention.NONE)
            .singleInput().typeName("LogicalProject").convention(Convention.NONE)
            .singleInput().typeName("DingoTableScan").convention(DingoConventions.DINGO);
    }

    @Test
    public void testInsertValues() throws SqlParseException {
        String sql = "insert into test values(1, 'Alice', 1.0)";
        RelRoot relRoot = parse(sql);
        LogicalValues values = (LogicalValues)
            Assert.relNode(relRoot.rel).typeName("LogicalTableModify").convention(Convention.NONE)
                .prop("operation", TableModify.Operation.INSERT)
                .singleInput().typeName("LogicalValues").convention(Convention.NONE)
                .getInstance();
        List<? extends List<RexLiteral>> tuples = values.getTuples();
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
    }

    @Test
    public void testUpdate() throws SqlParseException {
        String sql = "update test set amount = 2.0 where id = 1";
        RelRoot relRoot = parse(sql);
        Assert.relNode(relRoot.rel).typeName("LogicalTableModify").convention(Convention.NONE)
            .prop("operation", TableModify.Operation.UPDATE)
            .singleInput().typeName("LogicalProject").convention(Convention.NONE)
            .singleInput().typeName("LogicalFilter").convention(Convention.NONE)
            .singleInput().typeName("DingoTableScan").convention(DingoConventions.DINGO);
    }

    @Test
    public void testSort() throws SqlParseException {
        String sql = "select * from test order by name";
        RelRoot relRoot = parse(sql);
        Assert.relNode(relRoot.rel).typeName("LogicalSort").convention(Convention.NONE)
            .singleInput().typeName("LogicalProject").convention(Convention.NONE)
            .singleInput().typeName("DingoTableScan").convention(DingoConventions.DINGO);
    }

    @Test
    public void testLimit() throws SqlParseException {
        String sql = "select * from test limit 3";
        RelRoot relRoot = parse(sql);
        Assert.relNode(relRoot.rel).typeName("LogicalSort").convention(Convention.NONE)
            .singleInput().typeName("LogicalProject").convention(Convention.NONE)
            .singleInput().typeName("DingoTableScan").convention(DingoConventions.DINGO);
    }

    @Test
    public void testSortLimit() throws SqlParseException {
        String sql = "select * from test order by name limit 3";
        RelRoot relRoot = parse(sql);
        Assert.relNode(relRoot.rel).typeName("LogicalSort").convention(Convention.NONE)
            .singleInput().typeName("LogicalProject").convention(Convention.NONE)
            .singleInput().typeName("DingoTableScan").convention(DingoConventions.DINGO);
    }

    @Test
    public void testDelete() throws SqlParseException {
        String sql = "delete from test where id = 3";
        RelRoot relRoot = parse(sql);
        Assert.relNode(relRoot.rel).typeName("LogicalTableModify").convention(Convention.NONE)
            .prop("operation", TableModify.Operation.DELETE)
            .singleInput().typeName("LogicalProject").convention(Convention.NONE)
            .singleInput().typeName("LogicalFilter").convention(Convention.NONE)
            .singleInput().typeName("DingoTableScan").convention(DingoConventions.DINGO);
    }

    @Test
    public void testTransfer() throws SqlParseException {
        String sql = "insert into test1 select id as id1, id as id2, id as id3, name, amount from test";
        RelRoot relRoot = parse(sql);
        Assert.relNode(relRoot.rel).typeName("LogicalTableModify").convention(Convention.NONE)
            .singleInput().typeName("LogicalProject").convention(Convention.NONE)
            .singleInput().typeName("DingoTableScan").convention(DingoConventions.DINGO);
    }
}
