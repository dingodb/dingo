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
import io.dingodb.calcite.rel.dingo.DingoHashJoin;
import io.dingodb.calcite.rel.dingo.DingoRoot;
import io.dingodb.calcite.rel.dingo.DingoStreamingConverter;
import io.dingodb.calcite.rel.logical.LogicalDingoRoot;
import io.dingodb.calcite.rel.dingo.DingoRelOp;
import io.dingodb.calcite.rel.dingo.DingoScanWithRelOp;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.test.asserts.Assert;
import io.dingodb.test.asserts.AssertRelNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Properties;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestJoin {
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
    public void testJoin() throws SqlParseException {
        String sql = "select * from test join test1 on test.name = test1.id1";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalJoin.class).prop("joinType", JoinRelType.INNER)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        log.info("Graph of planner:\n {}", parser.getPlanner().toDot());
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoRelOp.class)
            .soleInput().isA(DingoHashJoin.class).prop("joinType", JoinRelType.INNER)
            .inputNum(2);
    }

    @Test
    public void testJoin1() throws SqlParseException {
        String sql = "select * from test, test1 where test.name = test1.id1";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalFilter.class)
            .soleInput().isA(LogicalJoin.class).prop("joinType", JoinRelType.INNER)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        AssertRelNode assertJoin = Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoRelOp.class)
            .soleInput().isA(DingoHashJoin.class).prop("joinType", JoinRelType.INNER).inputNum(2);
        assertJoin.input(0).isA(DingoStreamingConverter.class)
            .soleInput().isA(DingoScanWithRelOp.class);
        assertJoin.input(1).isA(DingoStreamingConverter.class)
            .soleInput().isA(DingoScanWithRelOp.class);
    }

    @Test
    public void testJoin2() throws SqlParseException {
        String sql = "select * from test join test1 on test.amount < test1.amount";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalJoin.class).prop("joinType", JoinRelType.INNER)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        AssertRelNode assertJoin = Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoRelOp.class)
            .soleInput().isA(DingoHashJoin.class).prop("joinType", JoinRelType.INNER).inputNum(2);
        assertJoin.input(0).isA(DingoStreamingConverter.class)
            .soleInput().isA(DingoScanWithRelOp.class);
        assertJoin.input(1).isA(DingoStreamingConverter.class)
            .soleInput().isA(DingoScanWithRelOp.class);
    }

    @Test
    public void testJoinFilter() throws SqlParseException {
        String sql = "select * from test join test1 on test.name = test1.id1 where test.amount > 3.0";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalFilter.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalJoin.class).prop("joinType", JoinRelType.INNER)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        log.info("Graph of planner:\n {}", parser.getPlanner().toDot());
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoRelOp.class)
            .soleInput().isA(DingoRelOp.class)
            .soleInput().isA(DingoHashJoin.class).prop("joinType", JoinRelType.INNER)
            .inputNum(2);
    }

    @Test
    public void testJoinLeft() throws SqlParseException {
        String sql = "select * from test left join test1 on test.name = test1.id1";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalJoin.class).prop("joinType", JoinRelType.LEFT)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoRelOp.class)
            .soleInput().isA(DingoHashJoin.class).prop("joinType", JoinRelType.LEFT)
            .inputNum(2);
    }

    @Test
    public void testJoinRight() throws SqlParseException {
        String sql = "select * from test right join test1 on test.name = test1.id1";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalJoin.class).prop("joinType", JoinRelType.RIGHT)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoRelOp.class)
            .soleInput().isA(DingoHashJoin.class).prop("joinType", JoinRelType.RIGHT)
            .inputNum(2);
    }
}
