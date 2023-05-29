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
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoRoot;
import io.dingodb.calcite.rel.DingoStreamingConverter;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.LogicalDingoRoot;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.test.asserts.Assert;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestGetByKeys {
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

    @Test
    public void testGetByKeys() throws SqlParseException {
        String sql = "select * from test1 where id0 = 1";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalFilter.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        DingoTableScan scan = (DingoTableScan) Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableScan.class)
            .getInstance();
        assertThat((scan).getFilter()).isNotNull();
        assertThat((scan).getSelection()).isEqualTo(TupleMapping.of(Arrays.asList(0, 1, 2, 3, 4)));
    }

    @Test
    public void testGetByKeys1() throws SqlParseException {
        String sql = "select * from test1 where id0 = 1 and id1 = 'A' and id2 = true";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalFilter.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        DingoGetByKeys getByKeys = (DingoGetByKeys) Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoGetByKeys.class)
            .getInstance();
        List<Object[]> keyTuples = TableUtils.getTuplesForKeyMapping(
            getByKeys.getPoints(),
            TableUtils.getTableDefinition(getByKeys.getTable())
        );
        assertThat(keyTuples)
            .containsExactlyInAnyOrder(new Object[]{1, "A", true});
    }

    @Test
    public void testGetByKeys2() throws SqlParseException {
        String sql = "select * from test1 where id0 = 1 and id1 = 'A' and not id2";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalFilter.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        DingoGetByKeys getByKeys = (DingoGetByKeys) Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoGetByKeys.class)
            .getInstance();
        List<Object[]> keyTuples = TableUtils.getTuplesForKeyMapping(
            getByKeys.getPoints(),
            TableUtils.getTableDefinition(getByKeys.getTable())
        );
        assertThat(keyTuples)
            .containsExactlyInAnyOrder(new Object[]{1, "A", false});
    }

    @Test
    public void testGetByKeys3() throws SqlParseException {
        String sql = "select * from test1 where (id0 = 1 or id0 = 2) and (id1 = 'A' or id1 = 'B') and id2";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalFilter.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        RelNode optimized = parser.optimize(relRoot.rel);
        DingoGetByKeys getByKeys = (DingoGetByKeys) Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoGetByKeys.class)
            .getInstance();
        List<Object[]> keyTuples = TableUtils.getTuplesForKeyMapping(
            getByKeys.getPoints(),
            TableUtils.getTableDefinition(getByKeys.getTable())
        );
        assertThat(keyTuples)
            .containsExactlyInAnyOrder(
                new Object[]{1, "A", true},
                new Object[]{1, "B", true},
                new Object[]{2, "A", true},
                new Object[]{2, "B", true}
            );
    }
}
