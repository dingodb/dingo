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

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface MetaService {
    String getName();

    void init(@Nullable Map<String, Object> props) throws Exception;

    void clear();

    default Object get(@Nonnull String name) {
        throw new IllegalArgumentException("Unsupported property name \"" + name + "\".");
    }

    void createTable(@Nonnull String tableName, @Nonnull TableDefinition tableDefinition);

    boolean dropTable(@Nonnull String tableName);

    byte[] getTableKey(@Nonnull String tableName);

    CommonId getTableId(@Nonnull String tableName);

    byte[] getIndexId(@Nonnull String tableName);

    Map<String, TableDefinition> getTableDefinitions();

    default TableDefinition getTableDefinition(String name) {
        return getTableDefinitions().get(name);
    }

    NavigableMap<ComparableByteArray, Part> getParts(String name);

    List<Location> getDistributes(String name);

    Location currentLocation();
}
