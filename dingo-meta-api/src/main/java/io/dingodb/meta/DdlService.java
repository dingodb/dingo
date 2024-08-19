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

package io.dingodb.meta;

import io.dingodb.common.CommonId;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface DdlService {
    static DdlService root() {
        return DdlServiceProvider.getDefault().root();
    }

    void createSchema(String schemaName, String connId);

    void dropSchema(SchemaInfo schemaInfo, String connId);

    void createTableWithInfo(String schemaName, String tableName, @NonNull TableDefinition tableDefinition, String connId, String sql);

    void dropTable(SchemaInfo schemaInfo, Long tableId, String tableName, String connId);

    void truncateTable(SchemaInfo schemaInfo, Table table, String connId);

    void addColumn(SchemaInfo schemaInfo, Table table, Column column, String connId);
    void dropColumn(long schemaId, String schemaName, Long tableId, String tableName, String column, String markDel, String relatedIndex, String connId);

    default void createIndex(String schemaName, String tableName, TableDefinition indexDef) {

    }

    default void dropIndex(String schemaName, String tableName, String indexName) {}

    InfoSchema getIsLatest();

    Table getTable(String schemaName, String tableName);

    Table getTable(CommonId id);
}
