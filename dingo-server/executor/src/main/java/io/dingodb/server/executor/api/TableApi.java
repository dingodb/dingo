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

package io.dingodb.server.executor.api;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.codec.CodeTag;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.IndexStatus;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.Part;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.ExecutorApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.client.connector.impl.ServiceConnector;
import io.dingodb.server.client.meta.service.MetaServiceClient;
import io.dingodb.server.executor.index.IndexExecutor;
import io.dingodb.server.executor.sidebar.TableSidebar;
import io.dingodb.server.executor.sidebar.TableStatus;
import io.dingodb.server.executor.store.LocalMetaStore;
import io.dingodb.server.protocol.meta.TablePart;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.dingodb.common.config.DingoConfiguration.location;
import static io.dingodb.common.config.DingoConfiguration.serverId;

@Slf4j
public class TableApi implements io.dingodb.server.api.TableApi {

    public static final TableApi INSTANCE = new TableApi();

    private final LocalMetaStore localMetaStore = LocalMetaStore.INSTANCE;
    private final Map<CommonId, TableSidebar> tables = new ConcurrentHashMap<>();

    private TableApi() {
    }

    @Override
    public CompletableFuture<Boolean> createTable(
        CommonId id, TableDefinition tableDefinition, Map<CommonId, Location> mirrors
    ) {
        log.info("New table [{}].", id);
        try {
            localMetaStore.saveTable(id, tableDefinition);
            TableSidebar tableSidebar = TableSidebar.create(id, mirrors, tableDefinition);
            tables.put(id, tableSidebar);
            return CompletableFuture.completedFuture(tableSidebar.start());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CommonId createIndex(CommonId id, Index index) {
        try {
            TableDefinition tableDefinition = getDefinition(id);
            index.setStatus(IndexStatus.BUSY);
            tableDefinition.addIndex(index);
            tableDefinition.increaseVersion();
            TableSidebar tableSidebar = tables.get(id);
            tableSidebar.updateDefinition(tableDefinition);
            io.dingodb.server.protocol.meta.Index metaIndex = indexToMetaIndex(id, index);
            tableSidebar.saveNewIndex(metaIndex);
            tableSidebar.startIndexes();

            while (!tableSidebar.getStatus().equals(TableStatus.RUNNING)) {
                Thread.sleep(1000);
            }

            tableSidebar.setBusy();
            MetaService metaService = MetaService.root().getSubMetaService(MetaService.DINGO_NAME);
            IndexExecutor indexExecutor = new IndexExecutor(id, (MetaServiceClient) metaService);
            NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions = MetaService.root().getParts(id);
            for (ByteArrayUtils.ComparableByteArray partId : partitions.keySet()) {
                Part part = partitions.get(partId);
                ServiceConnector partConnector = new ServiceConnector(part.getId(), part.getReplicates());
                ExecutorApi executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, partConnector);
                List<KeyValue> keyValues = executorApi
                    .getKeyValueByRange(null, null, id, part.getStartKey(), null);
                for (KeyValue keyValue : keyValues) {
                    indexExecutor.insertIndex(indexExecutor.getRow(keyValue, tableDefinition),
                        tableDefinition, index.getName());
                }
            }
            index.setStatus(IndexStatus.NORMAL);
            tableSidebar.updateDefinition(tableDefinition);
            tableSidebar.setRunning();
            return metaIndex.getId();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteIndex(CommonId id, String name) {
        tables.get(id).dropIndex(name);
    }

    @Override
    public boolean updateTableDefinition(CommonId id, TableDefinition tableDefinition) {
        TableSidebar tableSidebar = tables.get(id);
        tableSidebar.updateDefinition(tableDefinition);
        return true;
    }

    @Override
    public CommonId getIndexId(CommonId tableId, String indexName) {
        TableSidebar tableSidebar = tables.get(tableId);
        if (tableSidebar == null) {
            return null;
        }
        io.dingodb.server.protocol.meta.Index index = tableSidebar.getIndexes().get(indexName);
        if (index == null) {
            return null;
        }
        return index.getId();
    }

    private io.dingodb.server.protocol.meta.Index  indexToMetaIndex(CommonId id, Index index) {
        return io.dingodb.server.protocol.meta.Index.builder()
            .table(id)
            .name(index.getName())
            .columns(index.getColumns())
            .unique(index.isUnique())
            .status(index.getStatus())
            .build();
    }

    @Override
    public CompletableFuture<Void> deleteTable(CommonId id) {
        log.info("Delete table [{}].", id);
        localMetaStore.deleteTable(id);
        tables.get(id).destroy();
        return CompletableFuture.completedFuture(null);
    }

    public TableSidebar get(CommonId tableId) {
        return tables.get(tableId);
    }

    public void register(TableSidebar tableSidebar) {
        tables.put(tableSidebar.coreId(), tableSidebar);
    }

    public void unregister(CommonId id) {
        tables.remove(id);
    }

    @Override
    public TableDefinition getDefinition(CommonId tableId) {
        return get(tableId).getDefinition();
    }

    @Override
    public List<TablePart> partitions(CommonId tableId) {
        return tables.get(tableId).partitions();
    }

    public static Map<CommonId, Location> mirrors(ServiceConnector connector, CommonId tableId) {
        return ApiRegistry.getDefault().proxy(io.dingodb.server.api.TableApi.class, connector).mirrors(tableId);
    }

    @Override
    public Map<CommonId, Location> mirrors(CommonId tableId) {
        Map<CommonId, Location> mirrors = get(tableId).getMirrors().stream().collect(Collectors.toMap(
            meta -> new CommonId(serverId().type, serverId().domain, meta.id.seq),
            CoreMeta::location
        ));
        mirrors.put(serverId(), location());
        return mirrors;
    }

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, Part> getParts() {
        return null;
    }
}
