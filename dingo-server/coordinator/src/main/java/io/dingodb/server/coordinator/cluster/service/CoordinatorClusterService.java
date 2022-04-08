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

package io.dingodb.server.coordinator.cluster.service;

import io.dingodb.cluster.ClusterService;
import io.dingodb.meta.Location;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.server.api.ClusterServiceApi;
import io.dingodb.server.coordinator.GeneralId;
import io.dingodb.server.coordinator.context.CoordinatorContext;
import io.dingodb.server.coordinator.meta.ScheduleMetaAdaptor;
import io.dingodb.server.coordinator.resource.ResourceView;
import io.dingodb.server.coordinator.resource.impl.ExecutorView;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

@Slf4j
public class CoordinatorClusterService implements ClusterService, ClusterServiceApi {
    private static final CoordinatorClusterService INSTANCE = new CoordinatorClusterService();
    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
    private ScheduleMetaAdaptor scheduleMetaAdaptor;

    private CoordinatorClusterService() {
    }

    public static CoordinatorClusterService instance() {
        return INSTANCE;
    }

    public void init(CoordinatorContext context) {
        this.scheduleMetaAdaptor = context.scheduleMetaAdaptor();
        netService.apiRegistry().register(ClusterServiceApi.class, this);
    }

    @Override
    public List<Location> getComputingLocations() {
        List<Location> result = new ArrayList<>();
        Map<GeneralId, ? extends ResourceView> map = scheduleMetaAdaptor.namespaceView().resourceViews();
        for (Map.Entry<GeneralId, ? extends ResourceView> entry : map.entrySet()) {
            if (entry.getValue() instanceof ExecutorView) {
                ExecutorView executorView = (ExecutorView) entry.getValue();
                Endpoint endpoint = executorView.stats().getLocation();
                Location location = new Location(endpoint.getIp(), endpoint.getPort(), "");
                result.add(location);
            }
        }
        return result;
    }
}
