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
import io.dingodb.common.annotation.ApiDeclaration;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.meta.Part;
import io.dingodb.server.protocol.meta.Schema;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.NavigableMap;

public interface MetaServiceApi {

    @ApiDeclaration
    CommonId rootId();

    @ApiDeclaration
    void createSubMetaService(CommonId id, String name);

    @ApiDeclaration
    Map<String, Schema> getSubSchemas(CommonId id);

    @ApiDeclaration
    Schema getSubSchema(CommonId id, String name);

    @ApiDeclaration
    boolean dropSchema(CommonId id, String name);

    @ApiDeclaration
    void createTable(CommonId id, @NonNull String tableName, @NonNull TableDefinition tableDefinition);

    @ApiDeclaration
    boolean dropTable(CommonId id, @NonNull String tableName);

    @ApiDeclaration
    CommonId getTableId(CommonId id, @NonNull String tableName);

    @ApiDeclaration
    Map<String, TableDefinition> getTableDefinitions(CommonId id);

    @ApiDeclaration
    TableDefinition getTableDefinition(CommonId id, @NonNull String name);

    @ApiDeclaration
    TableDefinition getTableDefinition(CommonId id);

    @ApiDeclaration
    NavigableMap<ComparableByteArray, Part> getParts(CommonId id, String name);

    @ApiDeclaration
    boolean updateTableDefinition(TableDefinition tableDefinition);
}
