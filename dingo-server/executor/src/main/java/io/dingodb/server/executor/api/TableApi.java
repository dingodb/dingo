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
import io.dingodb.common.table.Index;
import io.dingodb.common.table.IndexStatus;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.Part;
import io.dingodb.server.executor.sidebar.TableSidebar;
import io.dingodb.server.executor.store.LocalMetaStore;
import io.dingodb.server.protocol.meta.TablePart;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

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
        TableDefinition tableDefinition = getDefinition(id);
        tableDefinition.addIndex(index);
        tableDefinition.increaseVersion();
        TableSidebar tableSidebar = tables.get(id);
        tableSidebar.updateDefinition(tableDefinition);
        io.dingodb.server.protocol.meta.Index metaIndex = indexToMetaIndex(id, index);
        tableSidebar.saveNewIndex(metaIndex);
        tableSidebar.startIndexes();

        return metaIndex.getId();
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
        return tableSidebar.getIndexes().get(indexName).getId();
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
        tables.put(tableSidebar.id(), tableSidebar);
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

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, Part> getParts() {
        return null;
    }
}
