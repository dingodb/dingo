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
import io.dingodb.calcite.rel.DingoRoot;
import io.dingodb.calcite.rel.DingoSort;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.test.asserts.Assert;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
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
public class TestSortLimit {
    private static DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        DingoParserContext context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME);
        parser = new DingoParser(context);
    }

    @Test
    public void testSort() throws SqlParseException {
        String sql = "select * from test order by name, amount desc";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .soleInput().isA(LogicalSort.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        DingoSort sort = (DingoSort) Assert.relNode(optimized).isA(DingoRoot.class)
            .soleInput().isA(DingoSort.class)
            .getInstance();
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        assertThat(collations.get(0))
            .hasFieldOrPropertyWithValue("fieldIndex", 1)
            .hasFieldOrPropertyWithValue("direction", ASCENDING)
            .hasFieldOrPropertyWithValue("nullDirection", LAST);
        assertThat(collations.get(1))
            .hasFieldOrPropertyWithValue("fieldIndex", 2)
            .hasFieldOrPropertyWithValue("direction", DESCENDING)
            .hasFieldOrPropertyWithValue("nullDirection", FIRST);
        assertThat(sort.fetch).isNull();
        assertThat(sort.offset).isNull();
    }

    @Test
    public void testOffsetLimit() throws SqlParseException {
        String sql = "select * from test limit 3 offset 2";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .soleInput().isA(LogicalSort.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        DingoSort sort = (DingoSort) Assert.relNode(optimized).isA(DingoRoot.class)
            .soleInput().isA(DingoSort.class)
            .getInstance();
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        assertThat(collations).isEmpty();
        assertThat(sort.fetch).isNotNull();
        assertThat(RexLiteral.intValue(sort.fetch)).isEqualTo(3);
        assertThat(sort.offset).isNotNull();
        assertThat(RexLiteral.intValue(sort.offset)).isEqualTo(2);
    }

    @Test
    public void testSortLimit() throws SqlParseException {
        String sql = "select * from test order by name limit 3";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel).isA(DingoRoot.class)
            .soleInput().isA(LogicalSort.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        DingoSort sort = (DingoSort) Assert.relNode(optimized).isA(DingoRoot.class)
            .soleInput().isA(DingoSort.class)
            .getInstance();
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        assertThat(collations.get(0))
            .hasFieldOrPropertyWithValue("fieldIndex", 1)
            .hasFieldOrPropertyWithValue("direction", ASCENDING)
            .hasFieldOrPropertyWithValue("nullDirection", LAST);
        assertThat(sort.fetch).isNotNull();
        assertThat(RexLiteral.intValue(sort.fetch)).isEqualTo(3);
        assertThat(sort.offset).isNull();
    }
}
