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

package io.dingodb.server.coordinator.api;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.Optional;
import io.dingodb.mpu.core.Core;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.MetaApi;
import io.dingodb.server.api.ServiceConnectApi;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.meta.adaptor.impl.ExecutorAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TableAdaptor;
import io.dingodb.server.coordinator.meta.service.DingoMetaService;
import io.dingodb.server.protocol.meta.Column;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.server.protocol.meta.ExecutorStats;
import io.dingodb.server.protocol.meta.Replica;
import io.dingodb.server.protocol.meta.Schema;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePart;
import io.dingodb.server.protocol.meta.TablePartStats;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.STATS_IDENTIFIER;

public class CoordinatorServerApi implements ServiceConnectApi, MetaApi {

    private final Core core;

    public CoordinatorServerApi(Core core) {
        this.core = core;
        ApiRegistry.getDefault().register(io.dingodb.server.api.ServiceConnectApi.class, this);
        ApiRegistry.getDefault().register(io.dingodb.server.api.MetaApi.class, this);
    }

    @Override
    public Location leader(CommonId ignore) {
        return Optional.mapOrNull(core.getPrimary(), __ -> __.location);
    }

    @Override
    public List<Location> getAll(CommonId ignore) {
        return Stream.of(core.meta, core.firstMirror, core.secondMirror)
            .filter(Objects::nonNull)
            .map(__ -> __.location)
            .collect(Collectors.toList());
    }

    @Override
    public CommonId tableId(String name) {
        return ((TableAdaptor)MetaAdaptorRegistry.getMetaAdaptor(Table.class))
            .getTableId(DingoMetaService.ROOT_ID, name);
    }

    @Override
    public Table table(CommonId tableId) {
        return MetaAdaptorRegistry.getMetaAdaptor(Table.class)
            .get(tableId);
    }

    @Override
    public List<Table> table() {
        return new ArrayList<>(MetaAdaptorRegistry.getMetaAdaptor(Table.class).getAll());
    }

    @Override
    public TableDefinition tableDefinition(CommonId tableId) {
        return ((TableAdaptor)MetaAdaptorRegistry.getMetaAdaptor(Table.class)).getDefinition(tableId);
    }

    @Override
    public TableDefinition tableDefinition(String tableName) {
        return ((TableAdaptor) MetaAdaptorRegistry.getMetaAdaptor(Table.class)).getDefinition(tableId(tableName));
    }

    @Override
    public Column column(CommonId columnId) {
        return MetaAdaptorRegistry.getMetaAdaptor(Column.class).get(columnId);
    }

    @Override
    public List<Column> columns(CommonId tableId) {
        return MetaAdaptorRegistry.getMetaAdaptor(Column.class).getByDomain(tableId.seq());
    }

    @Override
    public List<Column> columns(String tableName) {
        return columns(tableId(tableName));
    }

    @Override
    public Executor executor(Location location) {
        return ((ExecutorAdaptor)MetaAdaptorRegistry.getMetaAdaptor(Executor.class)).get(location);
    }

    @Override
    public Executor executor(CommonId executorId) {
        return MetaAdaptorRegistry.getMetaAdaptor(Executor.class).get(executorId);
    }

    @Override
    public ExecutorStats executorStats(CommonId executorId) {
        return MetaAdaptorRegistry.getStatsMetaAdaptor(ExecutorStats.class).getStats(
            new CommonId(ID_TYPE.stats, STATS_IDENTIFIER.executor, executorId.domain(), executorId.seq())
        );
    }

    @Override
    public Replica replica(CommonId replicaId) {
        return MetaAdaptorRegistry.getMetaAdaptor(Replica.class).get(replicaId);
    }

    @Override
    public List<Replica> replicas(CommonId tablePartId) {
        return MetaAdaptorRegistry.getMetaAdaptor(Replica.class).getByDomain(tablePartId.seq());
    }

    @Override
    public List<Replica> replicas(String tableName) {
        return replicas(tableId(tableName));
    }

    @Override
    public Schema schema(CommonId schemaId) {
        return MetaAdaptorRegistry.getMetaAdaptor(Schema.class).get(schemaId);
    }

    @Override
    public TablePart tablePart(CommonId tablePartId) {
        return MetaAdaptorRegistry.getMetaAdaptor(TablePart.class).get(tablePartId);
    }

    @Override
    public List<TablePart> tableParts(CommonId tableId) {
        return MetaAdaptorRegistry.getMetaAdaptor(TablePart.class).getByDomain(tableId.seq());
    }

    @Override
    public List<TablePart> tableParts(String tableName) {
        return tableParts(tableId(tableName));
    }

    @Override
    public TablePartStats tablePartStats(CommonId tablePartId) {
        return MetaAdaptorRegistry.getStatsMetaAdaptor(TablePartStats.class).getStats(
            new CommonId(ID_TYPE.stats, STATS_IDENTIFIER.part, tablePartId.domain(), tablePartId.seq())
        );
    }

}
