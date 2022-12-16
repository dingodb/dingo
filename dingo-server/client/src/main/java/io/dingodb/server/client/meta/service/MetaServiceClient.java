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

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.meta.MetaService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.protocol.meta.Schema;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Accessors(fluent = true)
public class MetaServiceClient implements MetaService {

    @Delegate
    private final MetaServiceApi api;
    private final CoordinatorConnector connector;

    @Getter
    private final CommonId id;
    @Getter
    private final String name;

    public MetaServiceClient() {
        this(CoordinatorConnector.getDefault());
    }

    public MetaServiceClient(CoordinatorConnector connector) {
        if (connector == null) {
            this.api = null;
            this.connector = null;
            this.id = null;
            this.name = null;
            return;
        }
        this.api = ApiRegistry.getDefault().proxy(MetaServiceApi.class, connector);
        this.connector = connector;
        this.id = api.rootId();
        this.name = MetaService.ROOT_NAME;
    }

    private MetaServiceClient(CommonId id, String name, CoordinatorConnector connector, MetaServiceApi api) {
        this.connector = connector;
        this.api = api;
        this.id = id;
        this.name = name;
    }

    @Override
    public Map<String, MetaService> getSubMetaServices(CommonId id) {
        return getSubSchemas(id).entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> new MetaServiceClient(e.getValue().getId(), e.getValue().getName(), connector, api))
            );
    }

    @Override
    public MetaService getSubMetaService(CommonId id, String name) {
        Schema subSchema = getSubSchema(id, name);
        return new MetaServiceClient(subSchema.getId(), subSchema.getName(), connector, api);
    }

    @Override
    public boolean dropSubMetaService(CommonId id, String name) {
        return api.dropSchema(id, name);
    }

    @Override
    public Location currentLocation() {
        return DingoConfiguration.location();
    }
}
