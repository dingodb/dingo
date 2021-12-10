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
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoPartScan;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TestPhysicalPlan {
    private static DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        parser = new DingoParser();
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
        Assert.relNode(relNode).typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoValues").convention(DingoConventions.ROOT);
    }

    @Test
    public void testFullScan() throws SqlParseException {
        String sql = "select * from test";
        RelNode relNode = parse(sql);
        RelOptNode r = Assert.relNode(relNode).typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoCoalesce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoPartScan").convention(DingoConventions.DISTRIBUTED)
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
            .typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoCoalesce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoPartScan").convention(DingoConventions.DISTRIBUTED)
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
            .typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoCoalesce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoPartScan").convention(DingoConventions.DISTRIBUTED)
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
            .typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoCoalesce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoPartScan").convention(DingoConventions.DISTRIBUTED)
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
            .typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoCoalesce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoGetByKeys").convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        assertThat((((DingoGetByKeys) r).getKeyTuples()))
            .containsExactlyInAnyOrder(new Object[]{1, "A", true});
    }

    @Test
    public void testGetByKeys2() throws SqlParseException {
        String sql = "select * from test1 where id0 = 1 and id1 = 'A' and not id2";
        RelNode relNode = parse(sql);
        RelOptNode r = Assert.relNode(relNode)
            .typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoCoalesce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoGetByKeys").convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        assertThat((((DingoGetByKeys) r).getKeyTuples()))
            .containsExactlyInAnyOrder(new Object[]{1, "A", false});
    }

    @Test
    public void testGetByKeys3() throws SqlParseException {
        String sql = "select * from test1 where (id0 = 1 or id0 = 2) and (id1 = 'A' or id1 = 'B') and id2";
        RelNode relNode = parse(sql);
        RelOptNode r = Assert.relNode(relNode)
            .typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoCoalesce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoGetByKeys").convention(DingoConventions.DISTRIBUTED)
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
        Assert.relNode(relNode).typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoReduce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoAggregate").convention(DingoConventions.DISTRIBUTED)
            .singleInput().typeName("DingoPartScan").convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testAggregateGroup() throws SqlParseException {
        String sql = "select name, count(*) from test group by name";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoReduce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoAggregate").convention(DingoConventions.DISTRIBUTED)
            .singleInput().typeName("DingoPartScan").convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testAggregateGroup1() throws SqlParseException {
        String sql = "select count(*) from test group by name";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode)
            .typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoProject").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoReduce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoAggregate").convention(DingoConventions.DISTRIBUTED)
            .singleInput().typeName("DingoPartScan").convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testInsertValues() throws SqlParseException {
        String sql = "insert into test values(1, 'Alice', 1.0)";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoCoalesce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoPartModify").convention(DingoConventions.DISTRIBUTED)
            .singleInput().typeName("DingoDistributedValues").convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testUpdate() throws SqlParseException {
        String sql = "update test set amount = amount + 2.0 where id = 1";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoCoalesce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoPartModify").convention(DingoConventions.DISTRIBUTED)
            .singleInput().typeName("DingoProject").convention(DingoConventions.DISTRIBUTED)
            .singleInput().typeName("DingoGetByKeys").convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    @Disabled
    public void testSort() throws SqlParseException {
        String sql = "select * from test order by name";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).typeName("EnumerableSort").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoCoalesce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoPartScan").convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    @Disabled
    public void testLimit() throws SqlParseException {
        String sql = "select * from test limit 3";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).typeName("EnumerableLimitSort").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoCoalesce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoPartScan").convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    @Disabled
    public void testSortLimit() throws SqlParseException {
        String sql = "select * from test order by name limit 3";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).typeName("EnumerableLimitSort").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoCoalesce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoPartScan").convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testDelete() throws SqlParseException {
        String sql = "delete from test where id = 3";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoCoalesce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoPartModify").convention(DingoConventions.DISTRIBUTED)
            .singleInput().typeName("DingoGetByKeys").convention(DingoConventions.DISTRIBUTED);
    }

    @Test
    public void testTransfer() throws SqlParseException {
        String sql = "insert into test1 select id as id1, id as id2, id as id3, name, amount from test";
        RelNode relNode = parse(sql);
        Assert.relNode(relNode).typeName("EnumerableRoot").convention(EnumerableConvention.INSTANCE)
            .singleInput().typeName("DingoCoalesce").convention(DingoConventions.ROOT)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.PARTITIONED)
            .singleInput().typeName("DingoPartModify").convention(DingoConventions.DISTRIBUTED)
            .singleInput().typeName("DingoExchange").convention(DingoConventions.DISTRIBUTED)
            .singleInput().typeName("DingoPartition").convention(DingoConventions.DISTRIBUTED)
            .singleInput().typeName("DingoProject").convention(DingoConventions.DISTRIBUTED)
            .singleInput().typeName("DingoPartScan").convention(DingoConventions.DISTRIBUTED);
    }
}
