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

package io.dingodb.server.coordinator.api;

import io.dingodb.common.Location;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.protocol.meta.Executor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class ClusterServiceApi implements io.dingodb.server.api.ClusterServiceApi {
    private static final ClusterServiceApi INSTANCE = new ClusterServiceApi();

    private ClusterServiceApi() {
    }

    public static ClusterServiceApi instance() {
        return INSTANCE;
    }

    @Override
    public List<Location> getComputingLocations() {
        return MetaAdaptorRegistry.getMetaAdaptor(Executor.class).getAll().stream()
            .map(executor -> new Location(executor.getHost(), executor.getPort()))
            .collect(Collectors.toList());
    }
}
