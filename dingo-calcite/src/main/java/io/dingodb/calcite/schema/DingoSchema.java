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

package io.dingodb.calcite.schema;

import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.runtime.DingoResource;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.MetaService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class DingoSchema extends AbstractSchema {

    DingoSchema(MetaService metaService, DingoParserContext context, List<String> parent) {
        super(metaService, context, parent);
    }

    public void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition) {
        metaService.createTable(tableName, tableDefinition);
        addTableCache(tableName, tableDefinition, Collections.emptyMap());
    }

    public void createTables(@NonNull TableDefinition tableDefinition,
                             @NonNull List<TableDefinition> indexTableDefinitions) {
        metaService.createTables(tableDefinition, indexTableDefinitions);
        CommonId tableId = metaService.getTableId(tableDefinition.getName());
        if (tableId == null) {
            throw new RuntimeException("Create table success, but not found table id.");
        }
        addTableCache(tableDefinition.getName(), tableDefinition, metaService.getTableIndexDefinitions(tableId));
    }

    public void updateTable(String tableName, TableDefinition definition) {
        metaService.updateTable(getTableId(tableName), definition);
        tableCache.remove(tableName);
    }

    public boolean dropTable(@NonNull String tableName) {
        // Get all index table commonIds
        CommonId tableId = metaService.getTableId(tableName);
        if (tableId == null) {
            return false;
        }
        List<CommonId> tableIds = Stream
            .concat(Stream.of(tableId), metaService.getTableIndexDefinitions(tableId).keySet().stream())
            .collect(Collectors.toList());
        if (metaService.dropTables(tableIds)) {
            tableCache.remove(tableName);
            return true;
        }
        return false;
    }

    public void createIndex(@NonNull String tableName, @NonNull TableDefinition indexDefinition) {
        CommonId tableId = getTableId(tableName);
        DingoTable table = (DingoTable) getTable(tableName);
        TableDefinition definition = table.getTableDefinition();
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(definition);
        metaService.createIndex(tableId, definition, indexDefinition);
        tableCache.remove(tableName);
        table = (DingoTable) getTable(tableName);
        recreateIndexData(tableName, indexDefinition.getName(), definition);
    }

    public void createDifferenceIndex(String tableName, String indexName, TableDefinition indexDefinition) {
        DingoTable table = (DingoTable) getTable(tableName);
        metaService.createDifferenceIndex(table.getTableId(), table.getIndexId(indexName), indexDefinition);
        tableCache.remove(tableName);
        table = (DingoTable) getTable(tableName);
        recreateIndexData(tableName, indexName, table.getTableDefinition());
    }

    public void dropIndex(@NonNull String tableName, @NonNull String index) {
        DingoTable table = (DingoTable) getTable(tableName);
        CommonId indexId = table.getIndexTableDefinitions().entrySet().stream()
            .filter(e -> e.getValue().getName().equalsIgnoreCase(index.toUpperCase()))
            .map(Map.Entry::getKey)
            .findAny().orElseThrow(() -> DingoResource.DINGO_RESOURCE.unknownIndex(index).ex());
        metaService.dropIndex(table.getTableId(), indexId);
        tableCache.remove(tableName);
        table = (DingoTable) getTable(tableName);
    }

    private void recreateIndexData(
        @NonNull String tableName, String indexName, @NonNull TableDefinition tableDefinition
    ) {
        try {
            KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(tableDefinition);
            CommonId tableId = getTableId(tableName);
            Collection<RangeDistribution> ranges = metaService.getRangeDistribution(tableId).values();
            StoreService storeService = StoreService.getDefault();
            for (RangeDistribution range : ranges) {
                StoreInstance store = storeService.getInstance(tableId, range.id());
                Iterator<KeyValue> iterator = store.scan(new StoreInstance.Range(
                    range.getStartKey(), range.getEndKey(), true, true
                ));
                while (iterator.hasNext()) {
                    store.insertIndex(codec.decode(iterator.next()));
                }
            }
        } catch (Exception e) {
            log.error("Recreate {} index date failed.", indexName, e);
            dropIndex(tableName, indexName);
            throw new RuntimeException(e);
        }
    }

    public Collection<Index> getIndex(@NonNull String tableName) {
        TableDefinition tableDefinition = metaService.getTableDefinition(tableName);
        if (tableDefinition != null && tableDefinition.getIndexes() != null) {
            return tableDefinition.getIndexes().values();
        } else {
            return Collections.emptyList();
        }
    }

    public void addDistribution(String tableName, PartitionDetailDefinition partitionDetail) {
        metaService.addDistribution(tableName, partitionDetail);
    }
}
