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

package io.dingodb.server.client.meta.service;

import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.meta.MetaService;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.ServiceLoader;
import javax.annotation.Nullable;

@Slf4j
public class MetaServiceClient implements MetaService {

    public static final Location CURRENT_LOCATION = new Location(DingoConfiguration.host(), DingoConfiguration.port());

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private final CoordinatorConnector connector;

    @Delegate
    private final MetaServiceApi metaServiceApi;

    public MetaServiceClient() {
        this(CoordinatorConnector.defaultConnector());
    }

    public MetaServiceClient(CoordinatorConnector connector) {
        this.connector = connector;
        metaServiceApi = netService.apiRegistry().proxy(MetaServiceApi.class, connector);
    }

    @Override
    public String getName() {
        return "DINGO";
    }

    @Override
    public void init(@Nullable Map<String, Object> props) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Location currentLocation() {
        return CURRENT_LOCATION;
    }

}
