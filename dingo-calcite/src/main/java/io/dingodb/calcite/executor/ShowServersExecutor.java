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

package io.dingodb.calcite.executor;

import io.dingodb.cluster.ClusterService;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowServersExecutor extends QueryExecutor {

    private ClusterService clusterService;

    public ShowServersExecutor() {
        clusterService = ClusterService.getDefault();
    }

    @Override
    Iterator<Object[]> getIterator() {
        List<Object[]> allNodes = new ArrayList<>();
        // Add executor (compute) nodes
        clusterService.getExecutors().stream()
            .map(e -> new Object[] {"executor", e.getId(), e.getHost(), e.getPort(), e.getState()})
            .forEach(allNodes::add);
        // Add store nodes
        clusterService.getStoreNodes().stream()
            .map(s -> new Object[] {"store", s[0], s[1], s[2], s[4]})
            .forEach(allNodes::add);
        // Add coordinator nodes: c[] = {id, host, port, state, isLeader}
        clusterService.getCoordinatorNodes().stream()
            .map(c -> new Object[] {"coordinator", c[0], c[1], c[2], c[3]})
            .forEach(allNodes::add);
        return allNodes.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("type");
        columns.add("id");
        columns.add("host");
        columns.add("port");
        columns.add("state");
        return columns;
    }
}
