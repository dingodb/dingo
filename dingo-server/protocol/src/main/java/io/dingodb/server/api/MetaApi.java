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
import io.dingodb.net.api.annotation.ApiDeclaration;
import io.dingodb.server.protocol.meta.Column;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.server.protocol.meta.ExecutorStats;
import io.dingodb.server.protocol.meta.Replica;
import io.dingodb.server.protocol.meta.Schema;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePart;
import io.dingodb.server.protocol.meta.TablePartStats;

import java.util.List;

public interface MetaApi {

    @ApiDeclaration
    CommonId tableId(String name);

    @ApiDeclaration
    Table table(CommonId tableId);

    @ApiDeclaration
    TableDefinition tableDefinition(CommonId tableId);

    @ApiDeclaration
    TableDefinition tableDefinition(String tableName);

    @ApiDeclaration
    Column column(CommonId columnId);

    @ApiDeclaration
    List<Column> columns(CommonId tableId);

    @ApiDeclaration
    List<Column> columns(String tableName);

    @ApiDeclaration
    Executor executor(Location location);

    @ApiDeclaration
    Executor executor(CommonId executorId);

    @ApiDeclaration
    ExecutorStats executorStats(CommonId executorId);

    @ApiDeclaration
    Replica replica(CommonId replicaId);

    @ApiDeclaration
    List<Replica> replicas(CommonId tablePartId);

    @ApiDeclaration
    List<Replica> replicas(String tableName);

    @ApiDeclaration
    Schema schema(CommonId schemaId);

    @ApiDeclaration
    TablePart tablePart(CommonId tablePartId);

    @ApiDeclaration
    List<TablePart> tableParts(CommonId tableId);

    @ApiDeclaration
    List<TablePart> tableParts(String tableName);

    @ApiDeclaration
    TablePartStats tablePartStats(CommonId tablePartId);

}
