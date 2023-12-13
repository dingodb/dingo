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

package io.dingodb.store.proxy.meta;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.MetaServiceProvider;
import io.dingodb.meta.Table;
import io.dingodb.meta.TableStatistic;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.meta.CreateSchemaRequest;
import io.dingodb.sdk.service.entity.meta.CreateTablesRequest;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.DropTablesRequest;
import io.dingodb.sdk.service.entity.meta.EntityType;
import io.dingodb.sdk.service.entity.meta.GenerateTableIdsRequest;
import io.dingodb.sdk.service.entity.meta.GetTableByNameResponse;
import io.dingodb.sdk.service.entity.meta.GetTablesRequest;
import io.dingodb.sdk.service.entity.meta.ReservedSchemaIds;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.sdk.service.entity.meta.TableIdWithPartIds;
import io.dingodb.store.proxy.Configuration;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;

public class MetaService implements io.dingodb.meta.MetaService {

    private static final io.dingodb.sdk.service.MetaService defaultStoreMeta = Services.metaService(
        Services.parse(Configuration.coordinators())
    );


    private static final String ROOT_NAME = "ROOT";
    private static final DingoCommonId ROOT_SCHEMA_ID = DingoCommonId.builder()
        .entityType(EntityType.ENTITY_TYPE_SCHEMA)
        .parentEntityId(ReservedSchemaIds.ROOT_SCHEMA.number.longValue())
        .entityId(ReservedSchemaIds.ROOT_SCHEMA.number.longValue())
        .build();

    public static final MetaService ROOT = new MetaService(defaultStoreMeta);

//    @AutoService(MetaServiceProvider.class)
    public static class Provider implements MetaServiceProvider {
        @Override
        public io.dingodb.meta.MetaService root() {
            return ROOT;
        }
    }


    private static Pattern pattern = Pattern.compile("^[A-Z_][A-Z\\d_]+$");
    private static Pattern warnPattern = Pattern.compile(".*[a-z]+.*");

    private final DingoCommonId id;
    private final String name;
    private final io.dingodb.sdk.service.MetaService service;

    public MetaService(io.dingodb.sdk.service.MetaService service) {
        this.service = service;
        this.id = ROOT_SCHEMA_ID;
        this.name = ROOT_NAME;
    }

    private MetaService(DingoCommonId id, String name, io.dingodb.sdk.service.MetaService service) {
        this.service = service;
        this.id = id;
        this.name = name;
    }

    @Override
    public Table getTable(CommonId tableId) {
        return null;
    }

    @Override
    public CommonId id() {
        return MAPPER.idFrom(id);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void createSubMetaService(String name) {
        if (id != ROOT_SCHEMA_ID) {
            throw new UnsupportedOperationException();
        }
        service.createSchema(CreateSchemaRequest.builder().parentSchemaId(id).schemaName(name).build());
    }

    @Override
    public Map<String, MetaService> getSubMetaServices() {
        if (id != ROOT_SCHEMA_ID) {
            return Collections.emptyMap();
        }
        return service.getSchemas(MAPPER.getSchemas(id)).getSchemas().stream()
            .map(schema -> new MetaService(schema.getId(), schema.getName(), service))
            .collect(Collectors.toMap(MetaService::name, Function.identity()));
    }

    @Override
    public MetaService getSubMetaService(String name) {
        if (id != ROOT_SCHEMA_ID) {
            return null;
        }
        return getSubMetaServices().get(name);
    }

    @Override
    public boolean dropSubMetaService(String name) {
        if (id != ROOT_SCHEMA_ID) {
            throw new UnsupportedOperationException();
        }
        service.dropSchema(MAPPER.dropSchema(getSubMetaService(name).id));
        return true;
    }

    @Override
    public void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTables(
        @NonNull TableDefinition tableDefinition, @NonNull List<TableDefinition> indexTableDefinitions
    ) {

        List<TableIdWithPartIds> tableIds = service.generateTableIds(GenerateTableIdsRequest.builder().build()).getIds();
        TableIdWithPartIds tableId = Optional.<TableIdWithPartIds>of(tableIds.stream()
            .filter(__ -> __.getTableId().getEntityType() == EntityType.ENTITY_TYPE_TABLE).findAny())
            .ifPresent(tableIds::remove)
            .orElseThrow(() -> new RuntimeException("Can not generate table id."));
        String tableName = tableDefinition.getName();
        List<TableDefinitionWithId> tables = Stream.concat(
                Stream.of(MAPPER.tableTo(tableId, tableDefinition)),
                indexTableDefinitions.stream()
                    .map(indexTableDefinition -> MAPPER.tableTo(tableIds.remove(0), indexTableDefinition))
                    .peek(td -> MAPPER.resetIndexParameter(td.getTableDefinition()))
                    .peek(td -> td.getTableDefinition().setName(tableName + "." + td.getTableDefinition().getName()))
            )
            .collect(Collectors.toList());
        service.createTables(CreateTablesRequest.builder().tableDefinitionWithIds(tables).build());
    }

    @Override
    public boolean dropTable(@NonNull String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropTables(@NonNull Collection<CommonId> tableIds) {
        service.dropTables(DropTablesRequest.builder()
            .tableIds(MAPPER.idTo(tableIds)).build()
        );
        return true;
    }

    @Override
    public Table getTable(String tableName) {
        TableDefinitionWithId tableWithId = service
            .getTableByName(MAPPER.getTableByName(id, tableName))
            .getTableDefinitionWithId();
        DingoCommonId tableId = tableWithId.getTableId();
        List<TableDefinitionWithId> tables = service.getTables(GetTablesRequest.builder().tableId(tableId).build())
            .getTableDefinitionWithIds().stream()
            .filter($ -> !$.getTableDefinition().getName().equalsIgnoreCase(tableName))
            .peek($ -> {
                String name = $.getTableDefinition().getName();
                String[] split = name.split("\\.");
                if (split.length > 1) {
                    name = split[split.length - 1];
                }
                $.getTableDefinition().setName(name);
            }).collect(Collectors.toList());

        return MAPPER.tableFrom(tableWithId, tables);
    }


    @Override
    public Table getTables() {
        return null;
    }

    @Override
    public CommonId getTableId(@NonNull String tableName) {
        return Optional.ofNullable(tableName)
            .filter($ -> !$.isEmpty())
            .map(name -> MAPPER.getTableByName(id, tableName))
            .map(service::getTableByName)
            .map(GetTableByNameResponse::getTableDefinitionWithId)
            .map(TableDefinitionWithId::getTableId)
            .map(MAPPER::idFrom)
            .orNull();
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
// TODO
//        return service.getTablesBySchema(MAPPER.getTablesBySchema(id)).getTableDefinitionWithIds().stream()
//            .map(TableDefinitionWithId::getTableDefinition)
//            .map(MAPPER::tableFrom)
//            .collect(Collectors.toMap(TableDefinition::getName, Function.identity()));
        return null;
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull String name) {
        return null;
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull CommonId id) {
        return null;
    }

    @Override
    public List<TableDefinition> getTableDefinitions(@NonNull String name) {
        return null;
    }

    @Override
    public Map<CommonId, TableDefinition> getTableIndexDefinitions(@NonNull String name) {
        return null;
    }

    @Override
    public Map<CommonId, TableDefinition> getTableIndexDefinitions(@NonNull CommonId id) {
        return null;
    }

    @Override
    public TableStatistic getTableStatistic(@NonNull String tableName) {
        return null;
    }

    @Override
    public Long getAutoIncrement(CommonId tableId) {
        return null;
    }

    @Override
    public Long getNextAutoIncrement(CommonId tableId) {
        return null;
    }
}
