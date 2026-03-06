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

import io.dingodb.calcite.grammar.dql.SqlShowCapacity;
import io.dingodb.calcite.grammar.dql.SqlShowComputeNodes;
import io.dingodb.calcite.grammar.dql.SqlShowCoordinatorNodes;
import io.dingodb.calcite.grammar.dql.SqlShowGcSafePoint;
import io.dingodb.calcite.grammar.dql.SqlShowRegions;
import io.dingodb.calcite.grammar.dql.SqlShowServers;
import io.dingodb.calcite.grammar.dql.SqlShowStoreJobs;
import io.dingodb.calcite.grammar.dql.SqlShowStoreNodes;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestShowClusterSqlNodes {

    @Test
    public void testSqlShowServersConstruction() {
        SqlShowServers node = new SqlShowServers(SqlParserPos.ZERO, null);
        assertNotNull(node);
        assertNull(node.sqlLikePattern);
        assertEquals(SqlKind.SELECT, node.getKind());
    }

    @Test
    public void testSqlShowServersWithPattern() {
        SqlShowServers node = new SqlShowServers(SqlParserPos.ZERO, "test%");
        assertNotNull(node);
        assertEquals("test%", node.sqlLikePattern);
    }

    @Test
    public void testSqlShowComputeNodesConstruction() {
        SqlShowComputeNodes node = new SqlShowComputeNodes(SqlParserPos.ZERO, null);
        assertNotNull(node);
        assertNull(node.sqlLikePattern);
        assertEquals(SqlKind.SELECT, node.getKind());
    }

    @Test
    public void testSqlShowComputeNodesWithPattern() {
        SqlShowComputeNodes node = new SqlShowComputeNodes(SqlParserPos.ZERO, "host%");
        assertNotNull(node);
        assertEquals("host%", node.sqlLikePattern);
    }

    @Test
    public void testSqlShowStoreNodesConstruction() {
        SqlShowStoreNodes node = new SqlShowStoreNodes(SqlParserPos.ZERO, null);
        assertNotNull(node);
        assertNull(node.sqlLikePattern);
        assertEquals(SqlKind.SELECT, node.getKind());
    }

    @Test
    public void testSqlShowStoreNodesWithPattern() {
        SqlShowStoreNodes node = new SqlShowStoreNodes(SqlParserPos.ZERO, "store%");
        assertNotNull(node);
        assertEquals("store%", node.sqlLikePattern);
    }

    @Test
    public void testSqlShowCoordinatorNodesConstruction() {
        SqlShowCoordinatorNodes node = new SqlShowCoordinatorNodes(SqlParserPos.ZERO, null);
        assertNotNull(node);
        assertNull(node.sqlLikePattern);
        assertEquals(SqlKind.SELECT, node.getKind());
    }

    @Test
    public void testSqlShowCoordinatorNodesWithPattern() {
        SqlShowCoordinatorNodes node = new SqlShowCoordinatorNodes(SqlParserPos.ZERO, "coord%");
        assertNotNull(node);
        assertEquals("coord%", node.sqlLikePattern);
    }

    @Test
    public void testSqlShowCapacityConstruction() {
        SqlShowCapacity node = new SqlShowCapacity(SqlParserPos.ZERO);
        assertNotNull(node);
        assertEquals(SqlKind.SELECT, node.getKind());
    }

    @Test
    public void testSqlShowRegionsConstruction() {
        SqlShowRegions node = new SqlShowRegions(SqlParserPos.ZERO);
        assertNotNull(node);
        assertEquals(SqlKind.SELECT, node.getKind());
    }

    @Test
    public void testSqlShowStoreJobsConstruction() {
        SqlShowStoreJobs node = new SqlShowStoreJobs(SqlParserPos.ZERO);
        assertNotNull(node);
        assertEquals(SqlKind.SELECT, node.getKind());
        assertNull(node.getJobId());
        assertNull(node.getArchiveLimit());
        assertEquals(false, node.isIncludeArchive());
        assertNull(node.getArchiveStartId());
    }

    @Test
    public void testSqlShowGcSafePointConstruction() {
        SqlShowGcSafePoint node = new SqlShowGcSafePoint(SqlParserPos.ZERO);
        assertNotNull(node);
        assertEquals(SqlKind.SELECT, node.getKind());
    }

    @Test
    public void testSqlShowServersOperandList() {
        SqlShowServers node = new SqlShowServers(SqlParserPos.ZERO, null);
        assertNull(node.getOperandList());
    }

    @Test
    public void testSqlShowCapacityOperandList() {
        SqlShowCapacity node = new SqlShowCapacity(SqlParserPos.ZERO);
        assertNull(node.getOperandList());
    }

    @Test
    public void testSqlShowRegionsOperandList() {
        SqlShowRegions node = new SqlShowRegions(SqlParserPos.ZERO);
        assertNull(node.getOperandList());
    }

    @Test
    public void testSqlShowGcSafePointOperandList() {
        SqlShowGcSafePoint node = new SqlShowGcSafePoint(SqlParserPos.ZERO);
        assertNull(node.getOperandList());
    }

    @Test
    public void testSqlShowStoreJobsWithParams() {
        SqlShowStoreJobs node = new SqlShowStoreJobs(SqlParserPos.ZERO, 123L, 10L, true, 5L);
        assertNotNull(node);
        assertEquals(SqlKind.SELECT, node.getKind());
        assertEquals(Long.valueOf(123L), node.getJobId());
        assertEquals(Long.valueOf(10L), node.getArchiveLimit());
        assertEquals(true, node.isIncludeArchive());
        assertEquals(Long.valueOf(5L), node.getArchiveStartId());
    }

    @Test
    public void testSqlShowStoreJobsOperandList() {
        SqlShowStoreJobs node = new SqlShowStoreJobs(SqlParserPos.ZERO);
        assertNull(node.getOperandList());
    }
}
