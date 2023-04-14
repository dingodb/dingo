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
import io.dingodb.common.config.DingoConfiguration;

import java.util.Collections;
import java.util.List;

public class ClusterService implements io.dingodb.cluster.ClusterService {

    public static final io.dingodb.cluster.ClusterService DEFAULT_INSTANCE = null;
    public static final List<Location> LOCAT_LOCATIONS = Collections.singletonList(DingoConfiguration.location());

    @AutoService(io.dingodb.cluster.ClusterServiceProvider.class)
    public static class ClusterServiceProvider implements io.dingodb.cluster.ClusterServiceProvider {
        @Override
        public io.dingodb.cluster.ClusterService get() {
            return DEFAULT_INSTANCE;
        }
    }

    @Override
    public List<Location> getComputingLocations() {
        return LOCAT_LOCATIONS;
    }

}
