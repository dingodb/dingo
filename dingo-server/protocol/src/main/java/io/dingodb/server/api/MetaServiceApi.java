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

package io.dingodb.server.api;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.meta.Part;
import io.dingodb.net.api.annotation.ApiDeclaration;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nonnull;

public interface MetaServiceApi {

    @ApiDeclaration
    void createTable(@Nonnull String tableName, @Nonnull TableDefinition tableDefinition);

    @ApiDeclaration
    boolean dropTable(@Nonnull String tableName);

    @ApiDeclaration
    byte[] getTableKey(@Nonnull String tableName);

    @ApiDeclaration
    CommonId getTableId(@Nonnull String tableName);

    @ApiDeclaration
    byte[] getIndexId(@Nonnull String tableName);

    @ApiDeclaration
    Map<String, TableDefinition> getTableDefinitions();

    @ApiDeclaration
    default TableDefinition getTableDefinition(String name) {
        return getTableDefinitions().get(name);
    }

    @ApiDeclaration
    NavigableMap<ComparableByteArray, Part> getParts(String name);

    @ApiDeclaration
    List<Location> getDistributes(String name);

}
