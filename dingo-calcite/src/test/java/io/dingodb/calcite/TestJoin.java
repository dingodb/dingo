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
import io.dingodb.calcite.mock.MockMetaServiceProvider;
import io.dingodb.calcite.rel.DingoCoalesce;
import io.dingodb.calcite.rel.DingoExchange;
import io.dingodb.calcite.rel.DingoFilter;
import io.dingodb.calcite.rel.DingoHashJoin;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.DingoRoot;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestJoin {
    private static DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        DingoParserContext context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME);
        parser = new DingoParser(context);
    }

    @Test
    public void testJoin() throws SqlParseException {
        String sql = "select * from test join test1 on test.name = test1.id1";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .singleInput().isA(LogicalProject.class)
            .singleInput().isA(LogicalJoin.class).prop("joinType", JoinRelType.INNER)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized).isA(DingoRoot.class)
            .singleInput().isA(DingoCoalesce.class)
            .singleInput().isA(DingoExchange.class).prop("root", true)
            .singleInput().isA(DingoProject.class)
            .singleInput().isA(DingoHashJoin.class).prop("joinType", JoinRelType.INNER)
            .inputNum(2);
    }

    @Test
    public void testJoin1() throws SqlParseException {
        String sql = "select * from test, test1 where test.name = test1.id1";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .singleInput().isA(LogicalProject.class)
            .singleInput().isA(LogicalFilter.class)
            .singleInput().isA(LogicalJoin.class).prop("joinType", JoinRelType.INNER)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized).isA(DingoRoot.class)
            .singleInput().isA(DingoCoalesce.class)
            .singleInput().isA(DingoExchange.class).prop("root", true)
            .singleInput().isA(DingoFilter.class)
            .singleInput().isA(DingoHashJoin.class).prop("joinType", JoinRelType.INNER)
            .inputNum(2);
    }

    @Test
    public void testJoin2() throws SqlParseException {
        String sql = "select * from test join test1 on test.amount < test1.amount";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .singleInput().isA(LogicalProject.class)
            .singleInput().isA(LogicalJoin.class).prop("joinType", JoinRelType.INNER)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized).isA(DingoRoot.class)
            .singleInput().isA(DingoCoalesce.class)
            .singleInput().isA(DingoExchange.class).prop("root", true)
            .singleInput().isA(DingoFilter.class)
            .singleInput().isA(DingoHashJoin.class).prop("joinType", JoinRelType.INNER)
            .inputNum(2);
    }

    @Test
    public void testJoinFilter() throws SqlParseException {
        String sql = "select * from test join test1 on test.name = test1.id1 where test.amount > 3.0";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .singleInput().isA(LogicalProject.class)
            .singleInput().isA(LogicalFilter.class)
            .singleInput().isA(LogicalProject.class)
            .singleInput().isA(LogicalJoin.class).prop("joinType", JoinRelType.INNER)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized).isA(DingoRoot.class)
            .singleInput().isA(DingoCoalesce.class)
            .singleInput().isA(DingoExchange.class).prop("root", true)
            .singleInput().isA(DingoFilter.class)
            .singleInput().isA(DingoProject.class)
            .singleInput().isA(DingoHashJoin.class).prop("joinType", JoinRelType.INNER)
            .inputNum(2);
    }

    @Test
    public void testJoinLeft() throws SqlParseException {
        String sql = "select * from test left join test1 on test.name = test1.id1";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .singleInput().isA(LogicalProject.class)
            .singleInput().isA(LogicalJoin.class).prop("joinType", JoinRelType.LEFT)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized).isA(DingoRoot.class)
            .singleInput().isA(DingoCoalesce.class)
            .singleInput().isA(DingoExchange.class).prop("root", true)
            .singleInput().isA(DingoProject.class)
            .singleInput().isA(DingoHashJoin.class).prop("joinType", JoinRelType.LEFT)
            .inputNum(2);
    }

    @Test
    public void testJoinRight() throws SqlParseException {
        String sql = "select * from test right join test1 on test.name = test1.id1";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .singleInput().isA(LogicalProject.class)
            .singleInput().isA(LogicalJoin.class).prop("joinType", JoinRelType.RIGHT)
            .inputNum(2);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized).isA(DingoRoot.class)
            .singleInput().isA(DingoCoalesce.class)
            .singleInput().isA(DingoExchange.class).prop("root", true)
            .singleInput().isA(DingoProject.class)
            .singleInput().isA(DingoHashJoin.class).prop("joinType", JoinRelType.RIGHT)
            .inputNum(2);
    }
}
