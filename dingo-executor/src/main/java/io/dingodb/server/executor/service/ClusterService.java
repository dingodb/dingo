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

package io.dingodb.server.executor.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.Location;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.sdk.common.cluster.Executor;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.cluster.ClusterServiceClient;
import io.dingodb.sdk.service.connector.CoordinatorServiceConnector;
import io.dingodb.server.executor.Configuration;
import io.dingodb.server.executor.common.ClusterHeartbeatExecutor;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ClusterService implements io.dingodb.cluster.ClusterService {

    public static final ClusterService DEFAULT_INSTANCE = new ClusterService();

    private static final CoordinatorServiceConnector connector = getCoordinatorServiceConnector();

    private static final ClusterServiceClient client = new ClusterServiceClient(connector);

    @AutoService(io.dingodb.cluster.ClusterServiceProvider.class)
    public static class ClusterServiceProvider implements io.dingodb.cluster.ClusterServiceProvider {
        @Override
        public io.dingodb.cluster.ClusterService get() {
            return DEFAULT_INSTANCE;
        }
    }

    @Override
    public List<Location> getComputingLocations() {
        return client.getExecutorMap(1).stream()
            .map(Executor::serverLocation)
            .map(location -> new Location(location.getHost(), location.getPort()))
            .collect(Collectors.toList());
    }

    public static void register() {
        Executors.scheduleWithFixedDelayAsync(
            "cluster-heartbeat",
            () -> client.executorHeartbeat(0, new ClusterHeartbeatExecutor()), 0, 1, TimeUnit.SECONDS
        );
    }

    public static CoordinatorServiceConnector getCoordinatorServiceConnector() {
        return Optional.ofNullable(Configuration.coordinators().split(","))
            .map(Arrays::stream)
            .map(ss -> ss
                .map(s -> s.split(":"))
                .map(__ -> new io.dingodb.sdk.common.Location(__[0], Integer.parseInt(__[1])))
                .collect(Collectors.toSet()))
            .map(CoordinatorServiceConnector::new)
            .orElseThrow("Create coordinator service connector error.");
    }
}
