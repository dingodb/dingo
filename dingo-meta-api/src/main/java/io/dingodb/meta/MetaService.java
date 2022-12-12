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
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.stream.Collectors;

public interface MetaService {

    static MetaService root() {
        return MetaServiceProvider.getRoot();
    }

    String ROOT_NAME = "DINGO";

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
    default void createSubMetaService(String name) {
        createSubMetaService(id(), name);
    }

    void createSubMetaService(CommonId id, String name);

    /**
     * Get all sub meta services.
     *
     * @return all sub meta services
     */
    default Map<String, MetaService> getSubMetaServices() {
        return getSubMetaServices(id());
    }

    Map<String, MetaService> getSubMetaServices(CommonId id);

    /**
     * Get sub meta service by name.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param name meta service name
     * @return sub meta service
     */
    default MetaService getSubMetaService(String name) {
        return getSubMetaServices().get(name);
    }

    MetaService getSubMetaService(CommonId id, String name);

    /**
     * Drop sub meta service by name.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     * @param name meta service name
     * @return true if success
     */
    default boolean dropSubMetaService(String name) {
        return dropSubMetaService(id(), name);
    }

    boolean dropSubMetaService(CommonId id, String name);

    /**
     * Create and save table meta, initialize table storage.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param tableName table name
     * @param tableDefinition table definition
     */
    default void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition) {
        createTable(id(), tableName, tableDefinition);
    }

    void createTable(CommonId id, @NonNull String tableName, @NonNull TableDefinition tableDefinition);

    /**
     * Drop table meta and table storage.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param tableName table name
     * @return true if success
     */
    default boolean dropTable(@NonNull String tableName) {
        return dropTable(id(), tableName);
    }

    boolean dropTable(CommonId id, @NonNull String tableName);

    /**
     * Get table id by table name.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param tableName table name
     * @return table id or null if not found
     */
    default CommonId getTableId(@NonNull String tableName) {
        return getTableId(id(), tableName);
    }

    CommonId getTableId(CommonId id, @NonNull String tableName);

    /**
     * Returns all table definition.
     *
     * @return all table definition
     */
    default Map<String, TableDefinition> getTableDefinitions() {
        return getTableDefinitions(id());
    }

    Map<String, TableDefinition> getTableDefinitions(CommonId id);

    /**
     * Get table definition by table name.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param name table name
     * @return table definition or null if not found.
     */
    default TableDefinition getTableDefinition(@NonNull String name) {
        return getTableDefinitions().get(name);
    }

    TableDefinition getTableDefinition(CommonId id, @NonNull String name);

    default boolean updateTableDefinition(@NonNull TableDefinition tableDefinition) {
        return updateTableDefinition(tableDefinition);
    }

    /**
     * Get table parts meta by table name.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param tableName table name
     * @return table parts meta
     */
    default NavigableMap<ComparableByteArray, Part> getParts(String tableName) {
        return getParts(id(), tableName);
    }

    NavigableMap<ComparableByteArray, Part> getParts(CommonId id, String name);

    /**
     * Get table storage locations by name.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param tableName table name
     * @return storage locations
     */
    default List<Location> getDistributes(String tableName) {
        return getParts(id(), tableName).values().stream()
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


}
