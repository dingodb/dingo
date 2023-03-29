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
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.Part;
import io.dingodb.meta.TableStatistic;
import io.dingodb.net.Message;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.net.service.ListenService;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.api.TableApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.client.connector.impl.ServiceConnector;
import io.dingodb.server.protocol.CommonIdConstant;
import io.dingodb.server.protocol.MetaListenEvent;
import io.dingodb.server.protocol.meta.Schema;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePart;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.server.protocol.ListenerTags.MetaListener.SCHEMA;
import static io.dingodb.server.protocol.ListenerTags.MetaListener.TABLE_DEFINITION;

@Slf4j
@Accessors(fluent = true)
public class MetaServiceClient implements MetaService {

    private static final ListenService listenService = ListenService.getDefault();

    private final Map<String, CommonId> metaServiceIdCache = new ConcurrentSkipListMap<>();
    private final Map<String, CommonId> tableIdCache = new ConcurrentHashMap<>();
    private final Map<CommonId, MetaServiceClient> metaServiceCache = new ConcurrentSkipListMap<>();
    private final Map<CommonId, TableDefinition> tableDefinitionCache = new ConcurrentSkipListMap<>();
    private final NavigableMap<CommonId, ServiceConnector> serviceCache = new ConcurrentSkipListMap<>();

    private final MetaServiceApi api;
    private final ServiceConnector connector;
    private ListenService.Future future;

    @Getter
    private CommonId id;
    @Getter
    private final String name;

    protected MetaServiceClient() {
        this(CoordinatorConnector.getDefault());
    }

    public MetaServiceClient(ServiceConnector connector) {
        Parameters.nonNull(connector, "connector");
        this.api = ApiRegistry.getDefault().proxy(MetaServiceApi.class, connector);
        this.connector = connector;
        this.name = MetaService.ROOT_NAME;
        Executors.execute("meta-service-client-reload", this::reload);
    }

    private MetaServiceClient(CommonId id, String name, ServiceConnector connector, MetaServiceApi api) {
        this.connector = connector;
        this.api = api;
        this.id = id;
        this.name = name;
        Executors.execute("meta-service-client-reload", this::reload);
    }

    private synchronized void reload() {
        if (!tableDefinitionCache.isEmpty() || !metaServiceCache.isEmpty()) {
            return;
        }
        try {
            if (id == null) {
                id = api.rootId();
            }
            future = listenService.listen(id, SCHEMA, connector.get(), this::onCallback, () -> {
                clearCache();
                reload();
            });
        } catch (Exception e) {
            Executors.scheduleAsync(id + "-reload", this::reload, 1, TimeUnit.SECONDS);
            log.error("Can not load [{}] meta service.", id, e);
            return;
        }
        api.getTableMetas(id).forEach(this::addTableCache);
        api.getSubSchemas(id).forEach(this::addSubMetaServiceCache);
    }

    private void close() {
        clearCache();
        future.cancel();
    }

    private synchronized void clearCache() {
        metaServiceIdCache.clear();
        tableIdCache.clear();
        tableDefinitionCache.clear();
        serviceCache.values().forEach(ServiceConnector::close);
        serviceCache.clear();
    }

    private synchronized void addSubMetaServiceCache(Schema schema) {
        metaServiceIdCache.computeIfAbsent(schema.getName(), __ -> schema.getId());
        metaServiceCache.computeIfAbsent(
            schema.getId(), __ -> new MetaServiceClient(schema.getId(), schema.getName(), connector, api)
        );
    }

    private synchronized void deleteSubMetaServiceCache(String name) {
        Optional.ifPresent(
            Optional.mapOrNull(metaServiceIdCache.remove(name), metaServiceCache::remove), MetaServiceClient::close
        );
    }

    private synchronized void addTableCache(Table table) {
        if (table == null) {
            return;
        }
        CommonId id = table.getId();
        String name = table.getName();
        tableIdCache.computeIfAbsent(name, __ -> id);
        tableDefinitionCache.put(id, table.getDefinition());
        ServiceConnector connector = serviceCache
            .computeIfAbsent(id, __ -> new ServiceConnector(id, api.getTableDistribute(id)));
        listenService.listen(id, TABLE_DEFINITION, connector.get(), this::onCallback, () -> deleteTableCache(name));
    }

    private synchronized void updateTableCache(TableDefinition definition) {
        tableDefinitionCache.put(tableIdCache.get(definition.getName()), definition);
    }

    private synchronized void deleteTableCache(String name) {
        Optional.ofNullable(tableIdCache.remove(name))
            .ifPresent(id -> serviceCache.remove(id).close())
            .ifPresent(tableDefinitionCache::remove)
            .map(this::partitionServices)
            .ifPresent(__ -> __.forEach(pid -> Optional.ifPresent(serviceCache.remove(pid), ServiceConnector::close)));
    }

    private Set<CommonId> partitionServices(CommonId tableId) {
        return serviceCache.subMap(
            CommonId.prefix(CommonIdConstant.ID_TYPE.table, CommonIdConstant.TABLE_IDENTIFIER.part, tableId.seq),
            true,
            CommonId.prefix(CommonIdConstant.ID_TYPE.table, CommonIdConstant.TABLE_IDENTIFIER.part, tableId.seq + 1),
            false
        ).keySet();
    }

    private void onCallback(Message message) {
        MetaListenEvent event = ProtostuffCodec.read(message.content());
        switch (event.event) {
            case CREATE_SCHEMA:
                addSubMetaServiceCache(event.meta());
                break;
            case CREATE_TABLE:
                addTableCache(event.meta());
                return;
            case UPDATE_SCHEMA:
                break;
            case UPDATE_TABLE:
                updateTableCache(event.meta());
                break;
            case DELETE_SCHEMA:
                deleteSubMetaServiceCache(event.meta());
                break;
            case DELETE_TABLE:
                deleteTableCache(event.meta());
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + event.event);
        }
    }

    public ServiceConnector getTableConnector(CommonId id) {
        return serviceCache.computeIfAbsent(id, __ -> new ServiceConnector(id, api.getTableDistribute(id)));
    }

    private ServiceConnector getPartConnector(CommonId tableId, CommonId partId) {
        return serviceCache.computeIfAbsent(
            partId, __ -> new ServiceConnector(partId, getTableConnector(tableId).getAddresses())
        );
    }

    @Override
    public void createSubMetaService(String name) {
        if (id == null) {
            throw new RuntimeException("Meta service not ready.");
        }
        api.createSubMetaService(id, name);
    }

    @Override
    public Map<String, MetaService> getSubMetaServices() {
        return metaServiceCache.values().stream()
            .collect(Collectors.toMap(MetaServiceClient::name, Function.identity()));
    }

    public MetaServiceClient getSubMetaService(CommonId id) {
        return metaServiceCache.get(id);
    }

    @Override
    public MetaService getSubMetaService(String name) {
        if (id == null) {
            throw new RuntimeException("Meta service not ready.");
        }
        MetaServiceClient subMetaService = Optional.mapOrNull(metaServiceIdCache.get(name), metaServiceCache::get);
        if (subMetaService == null) {
            Schema schema = api.getSubSchema(id, name);
            subMetaService = Optional.ofNullable(schema)
                .ifPresent(this::addSubMetaServiceCache)
                .map(Schema::getId)
                .mapOrNull(metaServiceCache::get);
        }
        return subMetaService;
    }

    @Override
    public boolean dropSubMetaService(String name) {
        return Optional.ofNullable(metaServiceIdCache.get(name))
            .map(api::dropSchema)
            .orElse(false);
    }

    @Override
    public synchronized void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition) {
        if (id == null) {
            throw new RuntimeException("Meta service not ready.");
        }
        CommonId tableId = api.createTable(id, tableName, tableDefinition);
        tableIdCache.put(tableName, tableId);
        tableDefinitionCache.put(tableId, tableDefinition);
    }

    @Override
    public synchronized boolean dropTable(@NonNull String tableName) {
        if (id == null) {
            throw new RuntimeException("Meta service not ready.");
        }
        return api.dropTable(id, tableName);
    }

    @Override
    public CommonId getTableId(@NonNull String tableName) {
        if (id == null) {
            throw new RuntimeException("Meta service not ready.");
        }
        CommonId id = tableIdCache.get(tableName);
        if (id == null) {
            addTableCache(api.getTableMeta(this.id, tableName));
            id = tableIdCache.get(tableName);
        }
        return id;
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
        if (tableDefinitionCache.isEmpty()) {
            reload();
        }
        return tableIdCache.values().stream().map(tableDefinitionCache::get)
            .collect(Collectors.toMap(TableDefinition::getName, Function.identity()));

    }

    @Override
    public TableDefinition getTableDefinition(@NonNull String name) {
        CommonId tableId = getTableId(name);
        return getTableDefinition(tableId);
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull CommonId id) {
        return ApiRegistry.getDefault().proxy(TableApi.class, getTableConnector(id)).getDefinition(id);
    }

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, Part> getParts(String tableName) {
        CommonId tableId = getTableId(tableName);
        return getParts(tableId);
    }

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, Part> getParts(CommonId id) {
        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> result = new TreeMap<>();
        ServiceConnector tableConnector = getTableConnector(id);
        ServiceConnector connector;
        for (TablePart tablePart : ApiRegistry.getDefault().proxy(TableApi.class, tableConnector).partitions(id)) {
            connector = getPartConnector(id, tablePart.getId());
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

    @Override
    public void createIndex(String tableName, List<Index> indexList) {
        try {
            CommonId tableId = getTableId(tableName);
            TableApi tableApi = ApiRegistry.getDefault().proxy(TableApi.class, getTableConnector(tableId));
            indexList.forEach(index -> {
                tableApi.createIndex(tableId, index);
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropIndex(String tableName, String indexName) {
        CommonId tableId = getTableId(tableName);
        TableApi tableApi = ApiRegistry.getDefault().proxy(TableApi.class, getTableConnector(tableId));
        tableApi.deleteIndex(tableId, indexName);
        TableDefinition tableDefinition = tableDefinitionCache.get(tableId);
        tableDefinition.removeIndex(indexName);
    }

    @Override
    public <T> T getTableProxy(Class<T> clazz, CommonId tableId) {
        ServiceConnector serviceConnector = getTableConnector(tableId);
        return ApiRegistry.getDefault().proxy(clazz, serviceConnector);
    }

    @Override
    public TableStatistic getTableStatistic(@NonNull String tableName) {
        return () -> 30000d;
    }
}
