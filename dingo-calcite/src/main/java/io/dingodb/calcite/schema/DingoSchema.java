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
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DingoSchema extends AbstractSchema {

    DingoSchema(MetaService metaService, DingoParserContext context, List<String> parent) {
        super(metaService, context, parent);
    }

    public void createTables(@NonNull TableDefinition tableDefinition,
                             @NonNull List<TableDefinition> indexTableDefinitions) {
        metaService.createTables(tableDefinition, indexTableDefinitions);
    }

    public boolean dropTable(@NonNull String tableName) {
        return metaService.dropTable(tableName);
    }

    public void addDistribution(String tableName, PartitionDetailDefinition partitionDetail) {
        metaService.addDistribution(tableName, partitionDetail);
    }
}
