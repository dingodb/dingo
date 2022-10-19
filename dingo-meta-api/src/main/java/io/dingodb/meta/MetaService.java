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
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public interface MetaService {
    String getName();

    void init(@Nullable Map<String, Object> props) throws Exception;

    void clear();

    default Object get(@NonNull String name) {
        throw new IllegalArgumentException("Unsupported property name \"" + name + "\".");
    }

    void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition);

    boolean dropTable(@NonNull String tableName);

    byte[] getTableKey(@NonNull String tableName);

    CommonId getTableId(@NonNull String tableName);

    byte[] getIndexId(@NonNull String tableName);

    Map<String, TableDefinition> getTableDefinitions();

    /**
     * get table definition by table name.
     *
     * @param name the input table name must be upper case.
     * @return TableDefinition or null if not found.
     */
    default TableDefinition getTableDefinition(@NonNull String name) {
        return getTableDefinitions().get(name.toUpperCase());
    }

    NavigableMap<ComparableByteArray, Part> getParts(String name);

    List<Location> getDistributes(String name);

    Location currentLocation();
}
