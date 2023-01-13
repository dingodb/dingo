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
import io.dingodb.common.Location;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.stream.Collectors;

public interface MetaService {

    static MetaService root() {
        return MetaServiceProvider.getDefault().root();
    }

    String ROOT_NAME = "DINGO_ROOT";
    String DINGO_NAME = "DINGO";
    String META_NAME = "META";

    /**
     * Returns this meta service id.
     *
     * @return this id
     */
    CommonId id();

    /**
     * Returns this meta service name.
     *
     * @return this name
     */
    String name();

    /**
     * Create sub meta service.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param name sub meta service name
     */
    void createSubMetaService(String name);


    /**
     * Get all sub meta services.
     *
     * @return all sub meta services
     */
    Map<String, MetaService> getSubMetaServices();

    /**
     * Get sub meta service by name.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param name meta service name
     * @return sub meta service
     */
    MetaService getSubMetaService(String name);

    /**
     * Drop sub meta service by name.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     * @param name meta service name
     * @return true if success
     */
    boolean dropSubMetaService(String name);

    /**
     * Create and save table meta, initialize table storage.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param tableName table name
     * @param tableDefinition table definition
     */
    void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition);

    /**
     * Drop table meta and table storage.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param tableName table name
     * @return true if success
     */
    boolean dropTable(@NonNull String tableName);

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

    /**
     * Get table parts meta by table name.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param tableName table name
     * @return table parts meta
     */
    NavigableMap<ComparableByteArray, Part> getParts(String tableName);

    /**
     * Get table parts meta by table id.
     *
     * @param id table id
     * @return table parts meta
     */
    NavigableMap<ComparableByteArray, Part> getParts(CommonId id);

    /**
     * Get table storage locations by name.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param tableName table name
     * @return storage locations
     */
    default List<Location> getDistributes(String tableName) {
        return getParts(tableName).values().stream()
            .map(Part::getLeader)
            .distinct()
            .collect(Collectors.toList());
    }

    /**
     * Returns current process location.
     *
     * @return current process location
     */
    Location currentLocation();

    void createIndex(String tableName, List<Index> indexList);

    void dropIndex(String tableName, String indexName);

}
