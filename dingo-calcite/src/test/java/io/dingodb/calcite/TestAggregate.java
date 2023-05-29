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
import io.dingodb.calcite.rel.DingoAggregate;
import io.dingodb.calcite.rel.DingoHashJoin;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.DingoReduce;
import io.dingodb.calcite.rel.DingoRoot;
import io.dingodb.calcite.rel.DingoStreamingConverter;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.LogicalDingoRoot;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.test.asserts.Assert;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestAggregate {
    private static DingoParserContext context;
    private DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        MockMetaServiceProvider.init();
        context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME);
    }

    @BeforeEach
    public void setup() {
        // Create each time to clean the statistic info.
        parser = new DingoParser(context);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "select count(*) from test",
        "select count(1) from test",
    })
    public void testCount(String sql) throws SqlParseException {
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalAggregate.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        log.info("Graph of planner:\n {}", parser.getPlanner().toDot());
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoReduce.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class);
    }

    @Test
    public void testCount1() throws SqlParseException {
        String sql = "select count(amount) from test";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalAggregate.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoReduce.class)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableScan.class);
    }

    @Test
    public void testDistinct() throws SqlParseException {
        String sql = "select distinct name from test";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalAggregate.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoReduce.class)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableScan.class);
    }

    @Test
    public void testCountGroup() throws SqlParseException {
        String sql = "select name, count(*) from test group by name";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalAggregate.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoReduce.class)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableScan.class);
    }

    @Test
    public void testCountGroup1() throws SqlParseException {
        String sql = "select count(*) from test group by name";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalAggregate.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoProject.class)
            .soleInput().isA(DingoReduce.class)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableScan.class);
    }

    @Test
    public void testDistinctCount() throws SqlParseException {
        String sql = "select count(distinct name) from test";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalAggregate.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoAggregate.class)
            .soleInput().isA(DingoReduce.class)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableScan.class);
    }

    @Test
    public void testSum() throws SqlParseException {
        String sql = "select sum(amount) from test";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalAggregate.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        try {
            RelNode optimized = parser.optimize(relRoot.rel);
            DingoTableScan scan = (DingoTableScan) Assert.relNode(optimized)
                .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
                .soleInput().isA(DingoReduce.class)
                .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
                .soleInput().isA(DingoTableScan.class)
                .getInstance();
            assertThat(scan.getAggCalls())
                .map(AggregateCall::getAggregation)
                .map(SqlAggFunction::getKind)
                .containsExactly(SqlKind.SUM);
        } catch (AssertionError e) {
            log.debug(parser.getPlanner().toDot());
        }
    }

    @Test
    public void testAvg() throws SqlParseException {
        String sql = "select avg(amount) from test";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalAggregate.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        DingoTableScan scan = (DingoTableScan) Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoProject.class)
            .soleInput().isA(DingoReduce.class)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableScan.class)
            .getInstance();
        assertThat(scan.getAggCalls())
            .map(AggregateCall::getAggregation)
            .map(SqlAggFunction::getKind)
            .containsExactly(SqlKind.SUM, SqlKind.COUNT);
    }

    @Test
    public void testAvg1() throws SqlParseException {
        String sql = "select name, avg(amount) from test group by name";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalAggregate.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        DingoTableScan scan = (DingoTableScan) Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoProject.class)
            .soleInput().isA(DingoReduce.class)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableScan.class)
            .getInstance();
        assertThat(scan.getAggCalls())
            .map(AggregateCall::getAggregation)
            .map(SqlAggFunction::getKind)
            .containsExactly(SqlKind.SUM, SqlKind.COUNT);
    }

    @Test
    public void testAvg2() throws SqlParseException {
        String sql = "select name, avg(id), avg(amount) from test group by name";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalAggregate.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        DingoTableScan scan = (DingoTableScan) Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoProject.class)
            .soleInput().isA(DingoReduce.class)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableScan.class)
            .getInstance();
        assertThat(scan.getAggCalls())
            .map(AggregateCall::getAggregation)
            .map(SqlAggFunction::getKind)
            .containsExactly(SqlKind.SUM, SqlKind.COUNT, SqlKind.SUM, SqlKind.COUNT);
    }

    @Test
    public void testSumAvg() throws Exception {
        String sql = "select sum(amount), avg(amount) from test";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalAggregate.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        DingoTableScan scan = (DingoTableScan) Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoProject.class)
            .soleInput().isA(DingoReduce.class)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableScan.class)
            .getInstance();
        assertThat(scan.getAggCalls())
            .map(AggregateCall::getAggregation)
            .map(SqlAggFunction::getKind)
            .containsExactly(SqlKind.SUM0, SqlKind.COUNT, SqlKind.SUM);
    }

    @Test
    public void testMultiDistinctCountWithGroup() throws SqlParseException {
        String sql = "select name, count(distinct id), count(distinct name) from test group by name";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalAggregate.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoProject.class)
            .soleInput().isA(DingoHashJoin.class).inputNum(2);
    }
}
