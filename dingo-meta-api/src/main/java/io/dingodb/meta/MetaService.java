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
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.meta.entity.Table;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

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

    default boolean isRoot() {
        return id().equals(root().id());
    }

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
    <M extends MetaService> Map<String, M> getSubMetaServices();

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
     *
     * @param name meta service name
     * @return true if success
     */
    boolean dropSubMetaService(String name);

    /**
     * Create and save table meta, initialize table storage.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param tableName       table name
     * @param tableDefinition table definition
     */
    @Deprecated
    default void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition) {
        createTables(tableDefinition, Collections.emptyList());
    }

    void createTables(@NonNull TableDefinition tableDefinition, @NonNull List<TableDefinition> indexTableDefinitions);

    /**
     * Drop table meta and table storage.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param tableName table name
     * @return true if success
     */
    boolean dropTable(@NonNull String tableName);

    boolean truncateTable(@NonNull String tableName);

//    boolean dropTables(@NonNull Collection<CommonId> tableIds);

    /**
     * Get table by table name.
     * Notice: check the table name case, because by default, the table names are converted to uppercase
     *
     * @param tableName table name
     * @return table definition or null if not found.
     */
    Table getTable(String tableName);

    Table getTable(CommonId tableId);

    /**
     * Returns all table.
     *
     * @return all table
     */
    Set<Table> getTables();

    default void addDistribution(String tableName, PartitionDetailDefinition detail) {
    }

    default Map<CommonId, Long> getTableCommitCount() {
        throw new UnsupportedOperationException();
    }

    /**
     * Get range distributions by table id.
     *
     * @param id table id
     * @return table range distributions
     */
    default NavigableMap<ComparableByteArray, RangeDistribution> getRangeDistribution(CommonId id) {
        // todo
        throw new UnsupportedOperationException();
    }

    /**
     * Returns current process location.
     *
     * @return current process location
     */
    default Location currentLocation() {
        return DingoConfiguration.location();
    }

    default void createIndex(String tableName, List<Index> indexList) {
        throw new UnsupportedOperationException();
    }

    default void dropIndex(String tableName, String indexName) {
        throw new UnsupportedOperationException();
    }

    TableStatistic getTableStatistic(@NonNull String tableName);

    TableStatistic getTableStatistic(@NonNull CommonId tableId);

    Long getAutoIncrement(CommonId tableId);

    Long getNextAutoIncrement(CommonId tableId);

    void updateAutoIncrement(CommonId tableId, long autoIncrementId);
}
