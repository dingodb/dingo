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
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.meta.Part;
import io.dingodb.net.api.annotation.ApiDeclaration;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public interface MetaServiceApi {

    @ApiDeclaration
    void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition);

    @ApiDeclaration
    boolean dropTable(@NonNull String tableName);

    @ApiDeclaration
    byte[] getTableKey(@NonNull String tableName);

    @ApiDeclaration
    CommonId getTableId(@NonNull String tableName);

    @ApiDeclaration
    byte[] getIndexId(@NonNull String tableName);

    @ApiDeclaration
    Map<String, TableDefinition> getTableDefinitions();

    @ApiDeclaration
    NavigableMap<ComparableByteArray, Part> getParts(String name);

    @ApiDeclaration
    List<Location> getDistributes(String name);

    @ApiDeclaration
    TableDefinition getTableDefinition(@NonNull CommonId commonId);

    @ApiDeclaration
    int registerUDF(CommonId id, String udfName, String function);

    @ApiDeclaration
    boolean unregisterUDF(CommonId id, String udfName, int version);

    @ApiDeclaration
    String getUDF(CommonId id, String udfName);

    @ApiDeclaration
    String getUDF(CommonId id, String udfName, int version);
}
