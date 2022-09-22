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
import io.dingodb.calcite.rel.DingoExchange;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoHash;
import io.dingodb.calcite.rel.DingoHashJoin;
import io.dingodb.calcite.rel.DingoPartModify;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.DingoRoot;
import io.dingodb.calcite.rel.DingoValues;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.test.asserts.Assert;
import io.dingodb.test.asserts.AssertRelNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Slf4j
public class TestUpdate {
    private static DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        DingoParserContext context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME);
        parser = new DingoParser(context);
    }

    @Test
    public void testUpdate() throws SqlParseException {
        String sql = "update test set amount = 2.0 where id = 1";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .soleInput().isA(LogicalTableModify.class).prop("operation", TableModify.Operation.UPDATE)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalFilter.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized).isA(DingoRoot.class)
            .soleInput().isA(DingoCoalesce.class)
            .soleInput().isA(DingoExchange.class).prop("root", true)
            .soleInput().isA(DingoPartModify.class)
            .soleInput().isA(DingoProject.class)
            .soleInput().isA(DingoGetByKeys.class);
    }

    @Test
    public void testUpdate1() throws SqlParseException {
        String sql = "update test set amount = amount + 2.0 where id = 1";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .soleInput().isA(LogicalTableModify.class).prop("operation", TableModify.Operation.UPDATE)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalFilter.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        Assert.relNode(optimized).isA(DingoRoot.class)
            .soleInput().isA(DingoCoalesce.class)
            .soleInput().isA(DingoExchange.class).prop("root", true)
            .soleInput().isA(DingoPartModify.class)
            .soleInput().isA(DingoProject.class)
            .soleInput().isA(DingoGetByKeys.class);
    }

    @Test
    @Disabled
    public void testUpdateWithMultiset() throws SqlParseException {
        String sql = "update `table-with-array` set `set` = multiset[1, 2, 3] where id = 1";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        AssertRelNode assertJoin = Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .soleInput().isA(LogicalTableModify.class).prop("operation", TableModify.Operation.UPDATE)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalJoin.class);
        assertJoin.input(0).isA(LogicalFilter.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        assertJoin.input(1).isA(Collect.class)
            .soleInput().isA(LogicalValues.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        assertJoin = Assert.relNode(optimized).isA(DingoRoot.class)
            .soleInput().isA(DingoCoalesce.class)
            .soleInput().isA(DingoExchange.class).prop("root", true)
            .soleInput().isA(DingoPartModify.class)
            .soleInput().isA(DingoProject.class)
            .soleInput().isA(DingoHashJoin.class);
        assertJoin.input(0).isA(DingoCoalesce.class)
            .soleInput().isA(DingoExchange.class)
            .soleInput().isA(DingoHash.class)
            .soleInput().isA(DingoGetByKeys.class);
        assertJoin.input(1).isA(DingoCoalesce.class)
            .soleInput().isA(DingoExchange.class)
            .soleInput().isA(DingoHash.class)
            .soleInput().isA(DingoValues.class);
    }
}
