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
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.Part;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.api.TableApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.client.connector.impl.ServiceConnector;
import io.dingodb.server.protocol.meta.TablePart;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Slf4j
@Accessors(fluent = true)
public class MetaServiceClient implements MetaService {

    private final Map<String, MetaServiceClient> subMetaServiceCache = new ConcurrentSkipListMap<>();
    private final Map<String, CommonId> tableIdCache = new HashMap<>();
    private final Map<String, TableDefinition> tableDefinitionCache = new ConcurrentSkipListMap<>();
    private final Map<CommonId, ServiceConnector> serviceCache = new ConcurrentSkipListMap<>();

    private final MetaServiceApi api;
    private final ServiceConnector connector;

    @Getter
    private final CommonId id;
    @Getter
    private final String name;

    public MetaServiceClient() {
        this(CoordinatorConnector.getDefault());
    }

    public MetaServiceClient(ServiceConnector connector) {
        Parameters.nonNull(connector, "connector");
        this.api = ApiRegistry.getDefault().proxy(MetaServiceApi.class, connector);
        this.connector = connector;
        this.id = api.rootId();
        this.name = MetaService.ROOT_NAME;
    }

    private MetaServiceClient(CommonId id, String name, ServiceConnector connector, MetaServiceApi api) {
        this.connector = connector;
        this.api = api;
        this.id = id;
        this.name = name;
    }

    private void load() {
        this.tableDefinitionCache.putAll(api.getTableDefinitions(id));
        api.getSubSchemas(id).forEach((key, value) ->
            this.subMetaServiceCache.put(key, new MetaServiceClient(value.getId(), value.getName(), connector, api))
        );
        // todo add listener
    }

    public ServiceConnector getTableConnector(CommonId id) {
        return serviceCache.computeIfAbsent(id, __ -> new ServiceConnector(id, api.getTableDistribute(id)));
    }

    private ServiceConnector getPartConnector(CommonId tableId, CommonId partId) {
        return serviceCache
            .computeIfAbsent(partId, __ -> new ServiceConnector(partId, getTableConnector(tableId).getAddresses()));
    }

    @Override
    public void createSubMetaService(String name) {
        api.createSubMetaService(id, name);
        subMetaServiceCache.put(name, new MetaServiceClient(id, name, connector, api));
    }

    @Override
    public Map<String, MetaService> getSubMetaServices() {
        return new HashMap<>(subMetaServiceCache);
    }

    @Override
    public MetaService getSubMetaService(String name) {
        return subMetaServiceCache.get(name);
    }

    @Override
    public boolean dropSubMetaService(String name) {
        api.dropSchema(id);
        subMetaServiceCache.remove(name);
        return true;
    }

    @Override
    public void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition) {
        tableIdCache.put(tableName, api.createTable(id, tableName, tableDefinition));
    }

    @Override
    public boolean dropTable(@NonNull String tableName) {
        return api.dropTable(id, tableName);
    }

    @Override
    public CommonId getTableId(@NonNull String tableName) {
        return tableIdCache.computeIfAbsent(tableName, __ -> api.getTableId(id, tableName));
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
        return api.getTableDefinitions(id);
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull String name) {
        CommonId tableId = getTableId(name);
        return ApiRegistry.getDefault().proxy(TableApi.class, getTableConnector(tableId)).getDefinition(tableId);
    }

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, Part> getParts(String tableName) {
        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> result = new TreeMap<>();
        CommonId tableId = getTableId(tableName);
        ServiceConnector tableConnector = getTableConnector(tableId);
        ServiceConnector connector;
        for (TablePart tablePart : ApiRegistry.getDefault().proxy(TableApi.class, tableConnector).partitions(tableId)) {
            connector = getPartConnector(tableId, tablePart.getId());
            Part part = new Part(
                tablePart.getId(),
                connector.get(),
                connector.getAddresses(),
                tablePart.getStart(),
                tablePart.getEnd()
            );
            result.put(new ByteArrayUtils.ComparableByteArray(part.getStartKey()), part);
        }
        return result;
    }

    @Override
    public Location currentLocation() {
        return DingoConfiguration.location();
    }
}
