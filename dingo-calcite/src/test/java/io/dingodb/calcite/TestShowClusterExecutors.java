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

import io.dingodb.calcite.executor.ShowCapacityExecutor;
import io.dingodb.calcite.executor.ShowComputeNodesExecutor;
import io.dingodb.calcite.executor.ShowCoordinatorNodesExecutor;
import io.dingodb.calcite.executor.ShowGcSafePointExecutor;
import io.dingodb.calcite.executor.ShowRegionsExecutor;
import io.dingodb.calcite.executor.ShowServersExecutor;
import io.dingodb.calcite.executor.ShowStoreJobsExecutor;
import io.dingodb.calcite.executor.ShowStoreNodesExecutor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestShowClusterExecutors {

    @Test
    public void testShowServersColumns() {
        ShowServersExecutor executor = new ShowServersExecutor();
        List<String> columns = executor.columns();
        assertNotNull(columns);
        assertEquals(5, columns.size());
        assertEquals(Arrays.asList("type", "id", "host", "port", "state"), columns);
    }

    @Test
    public void testShowComputeNodesColumns() {
        ShowComputeNodesExecutor executor = new ShowComputeNodesExecutor();
        List<String> columns = executor.columns();
        assertNotNull(columns);
        assertEquals(4, columns.size());
        assertEquals(Arrays.asList("id", "host", "port", "state"), columns);
    }

    @Test
    public void testShowStoreNodesColumns() {
        ShowStoreNodesExecutor executor = new ShowStoreNodesExecutor();
        List<String> columns = executor.columns();
        assertNotNull(columns);
        assertEquals(5, columns.size());
        assertEquals(Arrays.asList("id", "host", "port", "storeType", "state"), columns);
    }

    @Test
    public void testShowCoordinatorNodesColumns() {
        ShowCoordinatorNodesExecutor executor = new ShowCoordinatorNodesExecutor();
        List<String> columns = executor.columns();
        assertNotNull(columns);
        assertEquals(3, columns.size());
        assertEquals(Arrays.asList("host", "port", "state"), columns);
    }

    @Test
    public void testShowCapacityColumns() {
        ShowCapacityExecutor executor = new ShowCapacityExecutor();
        List<String> columns = executor.columns();
        assertNotNull(columns);
        assertEquals(3, columns.size());
        assertEquals(Arrays.asList("storeCount", "locationCount", "regionCount"), columns);
    }

    @Test
    public void testShowRegionsColumns() {
        ShowRegionsExecutor executor = new ShowRegionsExecutor();
        List<String> columns = executor.columns();
        assertNotNull(columns);
        assertEquals(1, columns.size());
        assertEquals(Arrays.asList("regionCount"), columns);
    }

    @Test
    public void testShowStoreJobsColumns() {
        ShowStoreJobsExecutor executor = new ShowStoreJobsExecutor();
        List<String> columns = executor.columns();
        assertNotNull(columns);
        assertEquals(2, columns.size());
        assertEquals(Arrays.asList("variable", "value"), columns);
    }

    @Test
    public void testShowGcSafePointColumns() {
        ShowGcSafePointExecutor executor = new ShowGcSafePointExecutor();
        List<String> columns = executor.columns();
        assertNotNull(columns);
        assertEquals(1, columns.size());
        assertEquals(Arrays.asList("gcSafePoint"), columns);
    }

}
