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

import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.MetaServiceProvider;
import io.dingodb.meta.TableStatistic;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.CoordinatorService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.Region;
import io.dingodb.sdk.service.entity.common.RegionDefinition;
import io.dingodb.sdk.service.entity.coordinator.GetRegionMapRequest;
import io.dingodb.sdk.service.entity.coordinator.QueryRegionRequest;
import io.dingodb.sdk.service.entity.meta.CreateSchemaRequest;
import io.dingodb.sdk.service.entity.meta.CreateTablesRequest;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.DropSchemaRequest;
import io.dingodb.sdk.service.entity.meta.DropTablesRequest;
import io.dingodb.sdk.service.entity.meta.EntityType;
import io.dingodb.sdk.service.entity.meta.GenerateTableIdsRequest;
import io.dingodb.sdk.service.entity.meta.GetSchemasRequest;
import io.dingodb.sdk.service.entity.meta.GetTableByNameRequest;
import io.dingodb.sdk.service.entity.meta.GetTableMetricsRequest;
import io.dingodb.sdk.service.entity.meta.GetTablesBySchemaRequest;
import io.dingodb.sdk.service.entity.meta.GetTablesRequest;
import io.dingodb.sdk.service.entity.meta.Partition;
import io.dingodb.sdk.service.entity.meta.ReservedSchemaIds;
import io.dingodb.sdk.service.entity.meta.Schema;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.sdk.service.entity.meta.TableIdWithPartIds;
import io.dingodb.sdk.service.entity.meta.TableWithPartCount;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.store.proxy.service.AutoIncrementService;
import io.dingodb.store.proxy.service.CodecService;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dingodb.common.CommonId.CommonType.TABLE;
import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;

@Slf4j
public class MetaService implements io.dingodb.meta.MetaService {

    private static final String ROOT_NAME = "ROOT";
    private static final DingoCommonId ROOT_SCHEMA_ID = DingoCommonId.builder()
        .entityType(EntityType.ENTITY_TYPE_SCHEMA)
        .parentEntityId(ReservedSchemaIds.ROOT_SCHEMA.number.longValue())
        .entityId(ReservedSchemaIds.ROOT_SCHEMA.number.longValue())
        .build();

    public static final MetaService ROOT = new MetaService();

//    @AutoService(MetaServiceProvider.class)
    public static class Provider implements MetaServiceProvider {
        @Override
        public io.dingodb.meta.MetaService root() {
            return ROOT;
        }
    }

    private static Pattern pattern = Pattern.compile("^[A-Z_][A-Z\\d_]+$");
    private static Pattern warnPattern = Pattern.compile(".*[a-z]+.*");

    public final DingoCommonId id;
    public final String name;
    public final io.dingodb.sdk.service.MetaService service;
    public final TsoService tsoService = TsoService.getDefault();
    public final MetaCache cache;

    public MetaService() {
        Set<Location> coordinators = Configuration.coordinatorSet();
        this.service = Services.metaService(coordinators);
        this.id = ROOT_SCHEMA_ID;
        this.name = ROOT_NAME;
        this.cache = new MetaCache(coordinators);
    }

    public MetaService(Set<Location> coordinators) {
        this.service = Services.metaService(coordinators);
        this.id = ROOT_SCHEMA_ID;
        this.name = ROOT_NAME;
        this.cache = new MetaCache(coordinators);
    }

    private MetaService(DingoCommonId id, String name, io.dingodb.sdk.service.MetaService service, MetaCache cache) {
        this.service = service;
        this.id = id;
        this.name = name;
        this.cache = cache;
    }

    private String cleanTableName(String name) {
        return cleanName(name, "Table");
    }

    private String cleanColumnName(String name) {
        return cleanName(name, "Column");
    }

    private String cleanSchemaName(String name) {
        return cleanName(name, "Schema");
    }

    private String cleanName(String name, String source) {
        if (warnPattern.matcher(name).matches()) {
            log.warn("{} name currently only supports uppercase letters, LowerCase -> UpperCase", source);
            name = name.toUpperCase();
        }
        if (!pattern.matcher(name).matches()) {
            throw new RuntimeException(source + " name currently only supports uppercase letters, digits, and underscores");
        }
        return name;
    }

    @Override
    public CommonId id() {
        return MAPPER.idFrom(id);
    }

    @Override
    public String name() {
        return name;
    }

    public long tso() {
        return tsoService.tso();
    }

    @Override
    public void createSubMetaService(String name) {
        if (id != ROOT_SCHEMA_ID) {
            throw new UnsupportedOperationException();
        }
        service.createSchema(tso(), CreateSchemaRequest.builder().parentSchemaId(id).schemaName(name).build());
    }

    @Override
    public Map<String, MetaService> getSubMetaServices() {
        if (id != ROOT_SCHEMA_ID) {
            return Collections.emptyMap();
        }
        return service.getSchemas(tso(), GetSchemasRequest.builder().schemaId(id).build()).getSchemas().stream()
            .map(schema -> new MetaService(schema.getId(), schema.getName(), service, cache))
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
        service.dropSchema(tso(), DropSchemaRequest.builder().schemaId(getSubMetaService(name).id).build());
        return true;
    }

    @Override
    public void createTables(
        @NonNull TableDefinition tableDefinition, @NonNull List<TableDefinition> indexTableDefinitions
    ) {
        cleanTableName(tableDefinition.getName());
        indexTableDefinitions.forEach($ -> cleanTableName($.getName()));
        // Generate new table ids.
        List<TableIdWithPartIds> tableIds = service.generateTableIds(tso(), GenerateTableIdsRequest.builder()
            .schemaId(id)
            .count(TableWithPartCount.builder()
                .hasTable(true)
                .indexCount(indexTableDefinitions.size())
                .tablePartCount(tableDefinition.getPartDefinition().getDetails().size())
                .indexPartCount(indexTableDefinitions.stream()
                    .map($ -> $.getPartDefinition().getDetails().size())
                    .collect(Collectors.toList())
                ).build()
            ).build()
        ).getIds();
        TableIdWithPartIds tableId = Optional.<TableIdWithPartIds>of(tableIds.stream()
            .filter(__ -> __.getTableId().getEntityType() == EntityType.ENTITY_TYPE_TABLE).findAny())
            .ifPresent(tableIds::remove)
            .orElseThrow(() -> new RuntimeException("Can not generate table id."));
        String tableName = tableDefinition.getName().toUpperCase();
        List<TableDefinitionWithId> tables = Stream.concat(
                Stream.of(MAPPER.tableTo(tableId, tableDefinition)),
                indexTableDefinitions.stream()
                    .map(indexTableDefinition -> MAPPER.tableTo(tableIds.remove(0), indexTableDefinition))
                    .peek(td -> MAPPER.resetIndexParameter(td.getTableDefinition()))
                    .peek(td -> td.getTableDefinition().setName(tableName + "." + td.getTableDefinition().getName()))
            )
            .collect(Collectors.toList());
        service.createTables(
            tso(), CreateTablesRequest.builder().schemaId(id).tableDefinitionWithIds(tables).build()
        );
    }

    @Override
    public boolean truncateTable(@NonNull String tableName) {
        // Get old table and indexes
        TableDefinitionWithId table = service.getTableByName(
            tso(), GetTableByNameRequest.builder().schemaId(id).tableName(tableName).build()
        ).getTableDefinitionWithId();
        List<TableDefinitionWithId> indexes = service.getTables(
                tso(), GetTablesRequest.builder().tableId(table.getTableId()).build()
        ).getTableDefinitionWithIds().stream()
            .filter($ -> !$.getTableDefinition().getName().equalsIgnoreCase(table.getTableDefinition().getName()))
            .collect(Collectors.toList());

        // Generate new table ids.
        List<TableIdWithPartIds> tableIds = service.generateTableIds(tso(), GenerateTableIdsRequest.builder()
            .schemaId(id)
            .count(TableWithPartCount.builder()
                .hasTable(true)
                .indexCount(indexes.size())
                .tablePartCount(table.getTableDefinition().getTablePartition().getPartitions().size())
                .indexPartCount(indexes.stream()
                    .map($ -> $.getTableDefinition().getTablePartition().getPartitions().size())
                    .collect(Collectors.toList())
                ).build()
            ).build()
        ).getIds();

        List<DingoCommonId> oldIds = new ArrayList<>();
        oldIds.add(table.getTableId());
        indexes.stream().map(TableDefinitionWithId::getTableId).forEach(oldIds::add);

        // Reset table id.
        TableIdWithPartIds newTableId = Optional.<TableIdWithPartIds>of(tableIds.stream()
                .filter(__ -> __.getTableId().getEntityType() == EntityType.ENTITY_TYPE_TABLE).findAny())
            .ifPresent(tableIds::remove)
            .orElseThrow(() -> new RuntimeException("Can not generate table id."));
        resetTableId(newTableId, table);


        //Reset indexes id.
        for (int i = 0; i < indexes.size(); i++) {
            resetTableId(tableIds.get(i), indexes.get(i));
        }
        List<TableDefinitionWithId> tables = new ArrayList<>();
        tables.add(table);
        tables.addAll(indexes);
        service.dropTables(tso(), DropTablesRequest.builder().tableIds(oldIds).build());
        cache.invalidTable(name, tableName);
        service.createTables(
            tso(),
            CreateTablesRequest.builder().schemaId(id).tableDefinitionWithIds(tables).build()
        );
        return true;
    }

    private void resetTableId(TableIdWithPartIds newTableId, TableDefinitionWithId table) {
        table.setTableId(newTableId.getTableId());
        List<Partition> partitions = table.getTableDefinition().getTablePartition().getPartitions();
        for (int i = 0; i < newTableId.getPartIds().size(); i++) {
            DingoCommonId partitionId = newTableId.getPartIds().get(i);
            partitions.get(i).setId(partitionId);
            CodecService.INSTANCE.setId(partitions.get(i).getRange().getStartKey(), partitionId.getEntityId());
            CodecService.INSTANCE.setId(partitions.get(i).getRange().getEndKey(), partitionId.getEntityId() + 1);
        }
    }

    @Override
    public boolean dropTable(@NonNull String tableName) {
        Table table = cache.getTable(name, tableName);
        if (table == null) {
            return false;
        }
        List<CommonId> indexIds = table.getIndexes().stream().map(Table::getTableId).collect(Collectors.toList());
        boolean result = dropTables(
            Stream.concat(Stream.of(table.getTableId()), indexIds.stream()).collect(Collectors.toList())
        );
        cache.invalidTable(name, tableName);
        cache.invalidTable(table.getTableId());
        indexIds.forEach(cache::invalidTable);
        return result;
    }

    public boolean dropTables(@NonNull Collection<CommonId> tableIds) {
        service.dropTables(tso(), DropTablesRequest.builder()
            .tableIds(MAPPER.idTo(tableIds)).build()
        );
        return true;
    }

    @Override
    public Table getTable(String tableName) {
        return cache.getTable(name, cleanTableName(tableName));
    }

    @Override
    public Table getTable(CommonId tableId) {
        return cache.getTable(tableId);
    }

    @Override
    public Set<Table> getTables() {
        List<TableDefinitionWithId> tablesWithIds = service.getTablesBySchema(
            tso(), GetTablesBySchemaRequest.builder().schemaId(id).build()
        ).getTableDefinitionWithIds();
        if (tablesWithIds == null) {
            return Collections.emptySet();
        }
        return tablesWithIds.stream()
            .map(tableWithId -> MAPPER.tableFrom(tableWithId, getIndexes(tableWithId, tableWithId.getTableId())))
            .collect(Collectors.toSet());
    }

    private List<TableDefinitionWithId> getIndexes(TableDefinitionWithId tableWithId, DingoCommonId tableId) {
        return service.getTables(tso(), GetTablesRequest.builder().tableId(tableId).build())
            .getTableDefinitionWithIds().stream()
            .filter($ -> !$.getTableDefinition().getName().equalsIgnoreCase(tableWithId.getTableDefinition().getName()))
            .peek($ -> {
                String name1 = $.getTableDefinition().getName();
                String[] split = name1.split("\\.");
                if (split.length > 1) {
                    name1 = split[split.length - 1];
                }
                $.getTableDefinition().setName(name1);
            }).collect(Collectors.toList());
    }

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> getRangeDistribution(CommonId id) {
        return cache.getRangeDistribution(id);
    }

    @Override
    public Map<CommonId, Long> getTableCommitCount() {
        if (!id.equals(ROOT_SCHEMA_ID)) {
            throw new UnsupportedOperationException("Only supported root meta service.");
        }

        long tso = tso();
        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
        List<Region> regions = coordinatorService.getRegionMap(tso, GetRegionMapRequest.builder().build()).getRegionmap().getRegions().stream()
            .map(Region::getId)
            .map($ -> coordinatorService.queryRegion(tso, QueryRegionRequest.builder().regionId($).build()).getRegion())
            .collect(Collectors.toList());
        List<Long> tableIds = service.getSchemas(tso, GetSchemasRequest.builder().build()).getSchemas().stream()
            .map(Schema::getTableIds)
            .flatMap(Collection::stream)
            .map(DingoCommonId::getEntityId)
            .collect(Collectors.toList());

        Map<CommonId, Long> metrics = new HashMap<>();

        for (Region region : regions) {
            RegionDefinition definition = region.getDefinition();
            if (!tableIds.contains(definition.getTableId())) {
                continue;
            }
            CommonId tableId = new CommonId(TABLE, definition.getSchemaId(), definition.getTableId());
            long committedIndex = region.getMetrics().getBraftStatus().getCommittedIndex();
            metrics.compute(tableId, (id, c) -> c == null ? committedIndex : c + committedIndex);
        }

        return metrics;
    }

    @Override
    public TableStatistic getTableStatistic(@NonNull String tableName) {
        return new TableStatistic() {

            private final long tso = tso();
            private final DingoCommonId tableId = service.getTableByName(
                tso(), GetTableByNameRequest.builder().schemaId(id).tableName(tableName).build()
            ).getTableDefinitionWithId().getTableId();

            @Override
            public byte[] getMinKey() {
                return service.getTableMetrics(
                    tso, GetTableMetricsRequest.builder().tableId(tableId).build()
                ).getTableMetrics().getTableMetrics().getMinKey();
            }

            @Override
            public byte[] getMaxKey() {
                return service.getTableMetrics(
                    tso, GetTableMetricsRequest.builder().tableId(tableId).build()
                ).getTableMetrics().getTableMetrics().getMaxKey();
            }

            @Override
            public long getPartCount() {
                return service.getTableMetrics(
                    tso, GetTableMetricsRequest.builder().tableId(tableId).build()
                ).getTableMetrics().getTableMetrics().getPartCount();
            }

            @Override
            public Double getRowCount() {
                return (double) service.getTableMetrics(
                    tso, GetTableMetricsRequest.builder().tableId(tableId).build()
                ).getTableMetrics().getTableMetrics().getRowsCount();
            }
        };
    }

    @Override
    public TableStatistic getTableStatistic(@NonNull CommonId tableId) {
        DingoCommonId id = MAPPER.idTo(tableId);
        return new TableStatistic() {

            private final long tso = tso();

            @Override
            public byte[] getMinKey() {
                return service.getTableMetrics(
                    tso, GetTableMetricsRequest.builder().tableId(id).build()
                ).getTableMetrics().getTableMetrics().getMinKey();
            }

            @Override
            public byte[] getMaxKey() {
                return service.getTableMetrics(
                    tso, GetTableMetricsRequest.builder().tableId(id).build()
                ).getTableMetrics().getTableMetrics().getMaxKey();
            }

            @Override
            public long getPartCount() {
                return service.getTableMetrics(
                    tso, GetTableMetricsRequest.builder().tableId(id).build()
                ).getTableMetrics().getTableMetrics().getPartCount();
            }

            @Override
            public Double getRowCount() {
                return (double) service.getTableMetrics(
                    tso, GetTableMetricsRequest.builder().tableId(id).build()
                ).getTableMetrics().getTableMetrics().getRowsCount();
            }
        };
    }

    @Override
    public Long getAutoIncrement(CommonId tableId) {
        return AutoIncrementService.INSTANCE.getAutoIncrement(tableId);
    }

    @Override
    public Long getNextAutoIncrement(CommonId tableId) {
        return AutoIncrementService.INSTANCE.getNextAutoIncrement(tableId);
    }

    @Override
    public void updateAutoIncrement(CommonId tableId, long autoIncrementId) {
        AutoIncrementService.INSTANCE.updateAutoIncrementId(tableId, autoIncrementId);
    }

}
