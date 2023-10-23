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
import io.dingodb.common.table.TableDefinition;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface SchemaService {

    /**
     * Create and save table meta, initialize table storage.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param tableName       table name
     * @param tableDefinition table definition
     */
    void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition);

    void createTables(@NonNull TableDefinition tableDefinition, @NonNull List<TableDefinition> indexTableDefinitions);

    default void updateTable(CommonId tableId, @NonNull TableDefinition tableDefinition) {
        throw new UnsupportedOperationException();
    }

    /**
     * Drop table meta and table storage.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param tableName table name
     * @return true if success
     */
    boolean dropTable(@NonNull String tableName);

    boolean dropTables(@NonNull Collection<CommonId> tableIds);

    /**
     * Get table id by table name.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param tableName table name
     * @return table id or null if not found
     */
    CommonId getTableId(@NonNull String tableName);

    /**
     * Returns all table definition.
     *
     * @return all table definition
     */
    Map<String, TableDefinition> getTableDefinitions();

    /**
     * Get table definition by table name.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param name table name
     * @return table definition or null if not found.
     */
    TableDefinition getTableDefinition(@NonNull String name);

    /**
     * Get table definition by table id.
     *
     * @param id table id
     * @return table definition or null if not found.
     */
    TableDefinition getTableDefinition(@NonNull CommonId id);




}
