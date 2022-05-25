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
import io.dingodb.calcite.mock.MockMetaServiceProvider;
import io.dingodb.calcite.rel.DingoAggregate;
import io.dingodb.calcite.rel.DingoCoalesce;
import io.dingodb.calcite.rel.DingoDistributedValues;
import io.dingodb.calcite.rel.DingoExchange;
import io.dingodb.calcite.rel.DingoExchangeRoot;
import io.dingodb.calcite.rel.DingoFilter;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoHashJoin;
import io.dingodb.calcite.rel.DingoPartModify;
import io.dingodb.calcite.rel.DingoPartScan;
import io.dingodb.calcite.rel.DingoPartition;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.DingoReduce;
import io.dingodb.calcite.rel.DingoSort;
import io.dingodb.calcite.rel.DingoValues;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptNode;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
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
        DingoParserContext context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME);
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
        Assert.relNode(relNode)
            .isA(DingoValues.class).convention(DingoConventions.ROOT);
    }

    @Test
    public void testFullScan() throws SqlParseException {
        String sql = "select * from test";
        RelNode relNode = parse(sql);
        RelOptNode r = Assert.relNode(relNode)
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
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
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
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
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
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
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
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
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
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
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
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
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
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
        Assert.relNode(relNode)
            .isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }


    @Test
    public void testDistinct() throws SqlParseException {
        String sql = "select distinct name from test";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode)
            .isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testDistinctCnt() throws SqlParseException {
        String sql = "select count(distinct name) from test";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode)
            .isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testAggregateGroup() throws SqlParseException {
        String sql = "select name, count(*) from test group by name";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode)
            .isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testAggregateGroup1() throws SqlParseException {
        String sql = "select count(*) from test group by name";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode)
            .isA(DingoProject.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testInsertValues() throws SqlParseException {
        String sql = "insert into test values(1, 'Alice', 1.0)";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode)
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartModify.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoDistributedValues.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testInsertValues1() throws SqlParseException {
        String sql = "insert into test values(1, 'Alice', 1.0), (2, 'Betty', 1.0 + 1.0)";
        RelNode relNode = parse(sql);
        Values values = (Values) Assert.relNode(relNode)
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartModify.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoDistributedValues.class).convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        List<? extends List<RexLiteral>> tuples = values.getTuples();
        assertThat(tuples).size().isEqualTo(2);
        assertThat(DataUtils.fromRexLiteral(tuples.get(1).get(2))).isEqualTo(BigDecimal.valueOf(2));
    }

    @Test
    public void testUpdate() throws SqlParseException {
        String sql = "update test set amount = amount + 2.0 where id = 1";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode)
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartModify.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoProject.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoGetByKeys.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testSort() throws SqlParseException {
        String sql = "select * from test order by name, amount desc";
        RelNode relNode = parse(sql);
        AssertRelNode assertSort = Assert.relNode(relNode);
        assertSort.isA(DingoSort.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
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
        AssertRelNode assertSort = Assert.relNode(relNode);
        assertSort.isA(DingoSort.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
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
        AssertRelNode assertSort = Assert.relNode(relNode);
        assertSort.isA(DingoSort.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
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
    public void testSum() throws SqlParseException {
        String sql = "select sum(amount) from test";
        RelNode relNode = parse(sql);
        AssertRelNode assertAgg = Assert.relNode(relNode)
            .isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED);
        DingoAggregate agg = (DingoAggregate) assertAgg.getInstance();
        assertThat(agg.getAggCallList())
            .map(AggregateCall::getAggregation)
            .map(SqlAggFunction::getKind)
            .containsExactly(SqlKind.SUM);
        assertAgg.singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testAvg() throws SqlParseException {
        String sql = "select avg(amount) from test";
        RelNode relNode = parse(sql);
        AssertRelNode assertAgg = Assert.relNode(relNode)
            .isA(DingoProject.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED);
        DingoAggregate agg = (DingoAggregate) assertAgg.getInstance();
        assertThat(agg.getAggCallList())
            .map(AggregateCall::getAggregation)
            .map(SqlAggFunction::getKind)
            .containsExactly(SqlKind.SUM, SqlKind.COUNT);
        assertAgg.singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testAvg1() throws SqlParseException {
        String sql = "select name, avg(amount) from test group by name";
        RelNode relNode = parse(sql);
        AssertRelNode assertAgg = Assert.relNode(relNode)
            .isA(DingoProject.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED);
        DingoAggregate agg = (DingoAggregate) assertAgg.getInstance();
        assertThat(agg.getAggCallList())
            .map(AggregateCall::getAggregation)
            .map(SqlAggFunction::getKind)
            .containsExactly(SqlKind.SUM, SqlKind.COUNT);
        assertAgg.singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testAvg2() throws SqlParseException {
        String sql = "select name, avg(id), avg(amount) from test group by name";
        RelNode relNode = parse(sql);
        AssertRelNode assertAgg = Assert.relNode(relNode)
            .isA(DingoProject.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED);
        DingoAggregate agg = (DingoAggregate) assertAgg.getInstance();
        assertThat(agg.getAggCallList())
            .map(AggregateCall::getAggregation)
            .map(SqlAggFunction::getKind)
            .containsExactly(SqlKind.SUM, SqlKind.COUNT, SqlKind.SUM, SqlKind.COUNT);
        assertAgg.singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testDelete() throws SqlParseException {
        String sql = "delete from test where id = 3";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode)
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartModify.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoGetByKeys.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testTransfer() throws SqlParseException {
        String sql = "insert into test1 select id as id1, id as id2, id as id3, name, amount from test";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode)
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartModify.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoExchange.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoPartition.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoProject.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testJoin() throws SqlParseException {
        String sql = "select * from test join test1 on test.name = test1.id1";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode)
            .isA(DingoProject.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoHashJoin.class).convention(DingoConventions.DISTRIBUTED)
            .inputNum(2);
    }

    @Test
    public void testJoin1() throws SqlParseException {
        String sql = "select * from test, test1 where test.name = test1.id1";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode)
            .isA(DingoFilter.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoHashJoin.class).convention(DingoConventions.DISTRIBUTED)
            .inputNum(2);
    }

    @Test
    public void testJoin2() throws SqlParseException {
        String sql = "select * from test join test1 on test.amount < test1.amount";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode)
            .isA(DingoFilter.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoHashJoin.class).convention(DingoConventions.DISTRIBUTED)
            .inputNum(2);
    }

    @Test
    public void testJoinFilter() throws SqlParseException {
        String sql = "select * from test join test1 on test.name = test1.id1 where test.amount > 3.0";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode)
            .isA(DingoFilter.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoProject.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoHashJoin.class).convention(DingoConventions.DISTRIBUTED)
            .inputNum(2);
    }

    @Test
    public void testSumAvg() throws Exception {
        String sql = "select sum(amount), avg(amount) from test";
        RelNode relNode = parse(sql);
        AssertRelNode assertAgg = Assert.relNode(relNode)
            .isA(DingoProject.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoReduce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoAggregate.class).convention(DingoConventions.DISTRIBUTED);
        DingoAggregate agg = (DingoAggregate) assertAgg.getInstance();
        assertThat(agg.getAggCallList())
            .map(AggregateCall::getAggregation)
            .map(SqlAggFunction::getKind)
            .containsExactly(SqlKind.SUM0, SqlKind.COUNT, SqlKind.SUM);
        assertAgg.singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testValuesReduce() throws Exception {
        String sql = "select a - b from (values (1, 2), (3, 5), (7, 11)) as t (a, b) where a + b > 4";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).isA(DingoValues.class);
        assertThat(((DingoValues) relNode).getValues()).containsExactly(new Object[]{-2}, new Object[]{-4});
    }

    @Test
    public void testIsNull() throws Exception {
        String sql = "select b from (values (1, 2), (null, 5), (7, 11)) as t (a, b) where a is null";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).isA(DingoValues.class);
        assertThat(((DingoValues) relNode).getValues()).containsExactly(new Object[]{5});
    }

    @Test
    public void testIsNotNull() throws Exception {
        String sql = "select b from (values (1, 2), (null, 5), (7, 11)) as t (a, b) where a is not null";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).isA(DingoValues.class);
        assertThat(((DingoValues) relNode).getValues()).containsExactly(new Object[]{2}, new Object[]{11});
    }

    @Test
    public void testInsertDateValues() throws Exception {
        String sql = "insert into `table-with-date`"
            + " values(1, 'Peso', '1970-1-1'), (2,'Alice','1970-1-2')";
        RelNode relNode = parse(sql);
        RelOptNode values = Assert.relNode(relNode)
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartModify.class).convention(DingoConventions.DISTRIBUTED)
            .singleInput().isA(DingoDistributedValues.class).convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        assertThat(((DingoDistributedValues) values).getValues()).containsExactly(
            new Object[]{1, "Peso", new Date(0L)},
            new Object[]{2, "Alice", new Date(24L * 60 * 60 * 1000)}
        );
    }

    @Test
    public void testDateValues() throws Exception {
        String sql = "select cast(a as date) from (values('1970-1-1')) as t (a)";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).isA(DingoValues.class);
        assertThat(((DingoValues) relNode).getValues()).containsExactly(new Object[]{new Date(0L)});
    }
}
