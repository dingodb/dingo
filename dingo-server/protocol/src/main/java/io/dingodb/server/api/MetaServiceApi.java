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

import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.Location;
import io.dingodb.meta.LocationGroup;
import io.dingodb.net.api.annotation.ApiDeclaration;

import java.util.Map;
import javax.annotation.Nonnull;

public interface MetaServiceApi {

    @ApiDeclaration
    void createTable(@Nonnull String tableName, @Nonnull TableDefinition tableDefinition);

    @ApiDeclaration
    boolean dropTable(@Nonnull String tableName);

    @ApiDeclaration
    byte[] getTableKey(@Nonnull String tableName);

    @ApiDeclaration
    byte[] getIndexId(@Nonnull String tableName);

    @ApiDeclaration
    Map<String, TableDefinition> getTableDefinitions();

    @ApiDeclaration
    TableDefinition getTableDefinition(String name);

    @ApiDeclaration
    Map<String, Location> getPartLocations(String name);

    @ApiDeclaration
    LocationGroup getLocationGroup(String name);

}
