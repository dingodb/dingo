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
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

@Slf4j
public class DingoSchema extends AbstractSchema {

    DingoSchema(MetaService metaService, DingoParserContext context, List<String> parent) {
        super(metaService, context, parent);
    }

    public void createTables(@NonNull TableDefinition tableDefinition,
                             @NonNull List<TableDefinition> indexTableDefinitions) {
        metaService.createTables(tableDefinition, indexTableDefinitions);
    }

    public void updateTable(String tableName, Table table) {
        metaService.updateTable(getTableId(tableName), table);
    }

    public boolean dropTable(@NonNull String tableName) {
        return metaService.dropTable(tableName);
    }

    public void createIndex(@NonNull String tableName, @NonNull TableDefinition indexDefinition) {
        CommonId tableId = getTableId(tableName);
        DingoTable table = getTable(tableName);
        Table definition = table.getTable();
        metaService.createIndex(tableId, TableDefinition.builder().name(definition.name).build(), indexDefinition);
    }

    public void createDifferenceIndex(String tableName, String indexName, IndexTable indexTable) {
        DingoTable table = getTable(tableName);
        metaService.createDifferenceIndex(table.getTableId(), table.getIndexId(indexName), indexTable);
    }

    public void dropIndex(@NonNull String tableName, @NonNull String index) {
        DingoTable table = getTable(tableName);
        CommonId indexId = table.getIndexTableDefinitions().stream()
            .filter(i -> i.name.equalsIgnoreCase(index.toUpperCase()))
            .findAny().orElseThrow(() -> DingoResource.DINGO_RESOURCE.unknownIndex(index).ex()).tableId;
        metaService.dropIndex(table.getTableId(), indexId);
    }

    public void addDistribution(String tableName, PartitionDetailDefinition partitionDetail) {
        metaService.addDistribution(tableName, partitionDetail);
    }
}
