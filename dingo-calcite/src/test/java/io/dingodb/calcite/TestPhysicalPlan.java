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
import io.dingodb.calcite.assertion.AssertRelNode;
import io.dingodb.calcite.rel.DingoAggregate;
import io.dingodb.calcite.rel.DingoCoalesce;
import io.dingodb.calcite.rel.DingoDistributedValues;
import io.dingodb.calcite.rel.DingoExchange;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoPartModify;
import io.dingodb.calcite.rel.DingoPartScan;
import io.dingodb.calcite.rel.DingoPartition;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.DingoReduce;
import io.dingodb.calcite.rel.DingoSort;
import io.dingodb.calcite.rel.DingoValues;
import io.dingodb.calcite.rel.EnumerableRoot;
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.op.FunFactory;
import io.dingodb.expr.parser.op.Op;
import io.dingodb.expr.parser.parser.DingoExprCompiler;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.RtExpr;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptNode;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING;
import static org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST;
import static org.apache.calcite.rel.RelFieldCollation.NullDirection.LAST;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TestPhysicalPlan {
    private static DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        DingoParserContext context = new DingoParserContext();
        parser = new DingoParser(context);
    }

    private static RelNode parse(String sql) throws SqlParseException {
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        RelNode relNode = parser.optimize(relRoot.rel);
        log.info("relNode = {}", relNode);
        return relNode;
    }

    @Test
    public void testSimple() throws SqlParseException {
        String sql = "select 1";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoValues.class).convention(DingoConventions.ROOT);
    }

    @Test
    public void testFullScan() throws SqlParseException {
        String sql = "select * from test";
        RelNode relNode = parse(sql);
        RelOptNode r = Assert.relNode(relNode).isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        DingoPartScan scan = (DingoPartScan) r;
        assertThat((scan).getFilter()).isNull();
        assertThat((scan).getSelection()).isNull();
    }

    @Test
    public void testFilterScan() throws SqlParseException {
        String sql = "select * from test where name = 'Alice' and amount > 3.0";
        RelNode relNode = parse(sql);
        RelOptNode r = Assert.relNode(relNode)
            .isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        DingoPartScan scan = (DingoPartScan) r;
        assertThat((scan).getFilter()).isNotNull();
        assertThat((scan).getSelection()).isNull();
    }

    @Test
    public void testProjectScan() throws SqlParseException {
        String sql = "select name, amount from test";
        RelNode relNode = parse(sql);
        RelOptNode r = Assert.relNode(relNode)
            .isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        DingoPartScan scan = (DingoPartScan) r;
        assertThat((scan).getFilter()).isNull();
        assertThat((scan).getSelection()).isNotNull();
    }

    @Test
    public void testGetByKeys() throws SqlParseException {
        String sql = "select * from test1 where id0 = 1";
        RelNode relNode = parse(sql);
        RelOptNode r = Assert.relNode(relNode)
            .isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        DingoPartScan scan = (DingoPartScan) r;
        assertThat((scan).getFilter()).isNotNull();
        assertThat((scan).getSelection()).isNull();
    }

    @Test
    public void testGetByKeys1() throws SqlParseException {
        String sql = "select * from test1 where id0 = 1 and id1 = 'A' and id2 = true";
        RelNode relNode = parse(sql);
        RelOptNode r = Assert.relNode(relNode)
            .isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoGetByKeys.class).convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        assertThat((((DingoGetByKeys) r).getKeyTuples()))
            .containsExactlyInAnyOrder(new Object[]{1, "A", true});
    }

    @Test
    public void testGetByKeys2() throws SqlParseException {
        String sql = "select * from test1 where id0 = 1 and id1 = 'A' and not id2";
        RelNode relNode = parse(sql);
        RelOptNode r = Assert.relNode(relNode)
            .isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoGetByKeys.class).convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        assertThat((((DingoGetByKeys) r).getKeyTuples()))
            .containsExactlyInAnyOrder(new Object[]{1, "A", false});
    }

    @Test
    public void testGetByKeys3() throws SqlParseException {
        String sql = "select * from test1 where (id0 = 1 or id0 = 2) and (id1 = 'A' or id1 = 'B') and id2";
        RelNode relNode = parse(sql);
        RelOptNode r = Assert.relNode(relNode)
            .isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoGetByKeys.class).convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        assertThat((((DingoGetByKeys) r).getKeyTuples()))
            .containsExactlyInAnyOrder(
                new Object[]{1, "A", true},
                new Object[]{1, "B", true},
                new Object[]{2, "A", true},
                new Object[]{2, "B", true}
            );
    }

    @Test
    public void testAggregate() throws SqlParseException {
        String sql = "select count(*) from test";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testAggregateGroup() throws SqlParseException {
        String sql = "select name, count(*) from test group by name";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testAggregateGroup1() throws SqlParseException {
        String sql = "select count(*) from test group by name";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode)
            .isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoProject.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testInsertValues() throws SqlParseException {
        String sql = "insert into test values(1, 'Alice', 1.0)";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartModify.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoDistributedValues.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testUpdate() throws SqlParseException {
        String sql = "update test set amount = amount + 2.0 where id = 1";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartModify.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoProject.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoGetByKeys.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testSort() throws SqlParseException {
        String sql = "select * from test order by name, amount desc";
        RelNode relNode = parse(sql);
        AssertRelNode assertSort = Assert.relNode(relNode)
            .isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE).singleInput();
        assertSort.isA(DingoSort.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
        DingoSort dingoSort = (DingoSort) assertSort.getInstance();
        List<RelFieldCollation> collations = dingoSort.getCollation().getFieldCollations();
        assertThat(collations.get(0))
            .hasFieldOrPropertyWithValue("fieldIndex", 1)
            .hasFieldOrPropertyWithValue("direction", ASCENDING)
            .hasFieldOrPropertyWithValue("nullDirection", LAST);
        assertThat(collations.get(1))
            .hasFieldOrPropertyWithValue("fieldIndex", 2)
            .hasFieldOrPropertyWithValue("direction", DESCENDING)
            .hasFieldOrPropertyWithValue("nullDirection", FIRST);
        assertThat(dingoSort.fetch).isNull();
        assertThat(dingoSort.offset).isNull();
    }

    @Test
    public void testOffsetLimit() throws SqlParseException {
        String sql = "select * from test limit 3 offset 2";
        RelNode relNode = parse(sql);
        AssertRelNode assertSort = Assert.relNode(relNode)
            .isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput();
        assertSort.isA(DingoSort.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
        DingoSort dingoSort = (DingoSort) assertSort.getInstance();
        List<RelFieldCollation> collations = dingoSort.getCollation().getFieldCollations();
        assertThat(collations).isEmpty();
        assertThat(dingoSort.fetch).isNotNull();
        assertThat(RexLiteral.intValue(dingoSort.fetch)).isEqualTo(3);
        assertThat(dingoSort.offset).isNotNull();
        assertThat(RexLiteral.intValue(dingoSort.offset)).isEqualTo(2);
    }

    @Test
    public void testSortLimit() throws SqlParseException {
        String sql = "select * from test order by name limit 3";
        RelNode relNode = parse(sql);
        AssertRelNode assertSort = Assert.relNode(relNode)
            .isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput();
        assertSort.isA(DingoSort.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
        DingoSort dingoSort = (DingoSort) assertSort.getInstance();
        List<RelFieldCollation> collations = dingoSort.getCollation().getFieldCollations();
        assertThat(collations.get(0))
            .hasFieldOrPropertyWithValue("fieldIndex", 1)
            .hasFieldOrPropertyWithValue("direction", ASCENDING)
            .hasFieldOrPropertyWithValue("nullDirection", LAST);
        assertThat(dingoSort.fetch).isNotNull();
        assertThat(RexLiteral.intValue(dingoSort.fetch)).isEqualTo(3);
        assertThat(dingoSort.offset).isNull();
    }

    @Test
    public void testAvg() throws SqlParseException {
        String sql = "select avg(amount) from test";
        RelNode relNode = parse(sql);
        AssertRelNode assertAgg = Assert.relNode(relNode)
            .isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoProject.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED);
        DingoAggregate agg = (DingoAggregate) assertAgg.getInstance();
        assertThat(agg.getAggList()).map(Object::getClass).map(Class::getSimpleName)
            .contains("SumAgg", "CountAgg");
        assertAgg.singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testAvg1() throws SqlParseException {
        String sql = "select name, avg(amount) from test group by name";
        RelNode relNode = parse(sql);
        AssertRelNode assertAgg = Assert.relNode(relNode)
            .isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoProject.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED);
        DingoAggregate agg = (DingoAggregate) assertAgg.getInstance();
        assertThat(agg.getAggList()).map(Object::getClass).map(Class::getSimpleName)
            .contains("SumAgg", "CountAgg");
        assertAgg.singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testAvg2() throws SqlParseException {
        String sql = "select name, avg(id), avg(amount) from test group by name";
        RelNode relNode = parse(sql);
        AssertRelNode assertAgg = Assert.relNode(relNode)
            .isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoProject.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED);
        DingoAggregate agg = (DingoAggregate) assertAgg.getInstance();
        assertThat(agg.getAggList()).map(Object::getClass).map(Class::getSimpleName)
            .contains("SumAgg", "CountAgg");
        assertAgg.singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testDelete() throws SqlParseException {
        String sql = "delete from test where id = 3";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartModify.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoGetByKeys.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testTransfer() throws SqlParseException {
        String sql = "insert into test1 select id as id1, id as id2, id as id3, name, amount from test";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).isA(EnumerableRoot.class).convention(EnumerableConvention.INSTANCE)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartModify.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoPartition.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoProject.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

}
