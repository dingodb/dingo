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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.dingo.DingoSqlParserImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestShowClusterSqlSyntax {

    private static final SqlParser.Config CONFIG =
        SqlParser.config().withParserFactory(DingoSqlParserImpl::new);

    private SqlNode parse(String sql) {
        SqlParser parser = SqlParser.create(sql, CONFIG);
        try {
            return parser.parseStmt();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testShowServers() {
        SqlNode sqlNode = parse("SHOW SERVERS");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowServers);
    }

    @Test
    public void testShowServersLowerCase() {
        SqlNode sqlNode = parse("show servers");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowServers);
    }

    @Test
    public void testShowComputeNodes() {
        SqlNode sqlNode = parse("SHOW COMPUTE_NODES");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowComputeNodes);
    }

    @Test
    public void testShowComputeNodesLowerCase() {
        SqlNode sqlNode = parse("show compute_nodes");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowComputeNodes);
    }

    @Test
    public void testShowStoreNodes() {
        SqlNode sqlNode = parse("SHOW STORE_NODES");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowStoreNodes);
    }

    @Test
    public void testShowStoreNodesLowerCase() {
        SqlNode sqlNode = parse("show store_nodes");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowStoreNodes);
    }

    @Test
    public void testShowCoordinatorNodes() {
        SqlNode sqlNode = parse("SHOW COORDINATOR_NODES");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowCoordinatorNodes);
    }

    @Test
    public void testShowCoordinatorNodesLowerCase() {
        SqlNode sqlNode = parse("show coordinator_nodes");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowCoordinatorNodes);
    }

    @Test
    public void testShowCapacity() {
        SqlNode sqlNode = parse("SHOW CAPACITY");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowCapacity);
    }

    @Test
    public void testShowCapacityLowerCase() {
        SqlNode sqlNode = parse("show capacity");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowCapacity);
    }

    @Test
    public void testShowRegionsCount() {
        SqlNode sqlNode = parse("SHOW REGIONS_COUNT");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowRegions);
    }

    @Test
    public void testShowRegionsCountLowerCase() {
        SqlNode sqlNode = parse("show regions_count");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowRegions);
    }

    @Test
    public void testShowStoreJobs() {
        SqlNode sqlNode = parse("SHOW STORE_JOBS");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowStoreJobs);
        SqlShowStoreJobs showStoreJobs = (SqlShowStoreJobs) sqlNode;
        assertNull(showStoreJobs.getJobId());
        assertNull(showStoreJobs.getArchiveLimit());
        assertEquals(false, showStoreJobs.isIncludeArchive());
        assertNull(showStoreJobs.getArchiveStartId());
    }

    @Test
    public void testShowStoreJobsLowerCase() {
        SqlNode sqlNode = parse("show store_jobs");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowStoreJobs);
    }

    @Test
    public void testShowStoreJobsWithJobId() {
        SqlNode sqlNode = parse("SHOW STORE_JOBS 123");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowStoreJobs);
        SqlShowStoreJobs showStoreJobs = (SqlShowStoreJobs) sqlNode;
        assertEquals(Long.valueOf(123L), showStoreJobs.getJobId());
    }

    @Test
    public void testShowStoreJobsWithIncludeArchive() {
        SqlNode sqlNode = parse("SHOW STORE_JOBS INCLUDE ARCHIVE");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowStoreJobs);
        SqlShowStoreJobs showStoreJobs = (SqlShowStoreJobs) sqlNode;
        assertTrue(showStoreJobs.isIncludeArchive());
    }

    @Test
    public void testShowStoreJobsWithLimit() {
        SqlNode sqlNode = parse("SHOW STORE_JOBS LIMIT 10");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowStoreJobs);
        SqlShowStoreJobs showStoreJobs = (SqlShowStoreJobs) sqlNode;
        assertEquals(Long.valueOf(10L), showStoreJobs.getArchiveLimit());
    }

    @Test
    public void testShowStoreJobsWithAllParams() {
        SqlNode sqlNode = parse("SHOW STORE_JOBS 42 INCLUDE ARCHIVE LIMIT 20 FROM 5");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowStoreJobs);
        SqlShowStoreJobs showStoreJobs = (SqlShowStoreJobs) sqlNode;
        assertEquals(Long.valueOf(42L), showStoreJobs.getJobId());
        assertTrue(showStoreJobs.isIncludeArchive());
        assertEquals(Long.valueOf(20L), showStoreJobs.getArchiveLimit());
        assertEquals(Long.valueOf(5L), showStoreJobs.getArchiveStartId());
    }

    @Test
    public void testShowGcSafePoint() {
        SqlNode sqlNode = parse("SHOW GC_SAFEPOINT");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowGcSafePoint);
    }

    @Test
    public void testShowGcSafePointLowerCase() {
        SqlNode sqlNode = parse("show gc_safepoint");
        assertNotNull(sqlNode);
        assertTrue(sqlNode instanceof SqlShowGcSafePoint);
    }
}
