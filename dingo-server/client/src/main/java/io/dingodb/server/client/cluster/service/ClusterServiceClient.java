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

package io.dingodb.server.client.cluster.service;

import io.dingodb.cluster.ClusterService;
import io.dingodb.common.Location;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.api.ClusterServiceApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

@Slf4j
public class ClusterServiceClient implements ClusterService {
    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    @Delegate
    private ClusterServiceApi clusterServiceApi;

    public ClusterServiceClient() {
        this(CoordinatorConnector.defaultConnector());
    }

    public ClusterServiceClient(CoordinatorConnector connector) {
        clusterServiceApi = netService.apiRegistry().proxy(ClusterServiceApi.class, connector);
    }
}
