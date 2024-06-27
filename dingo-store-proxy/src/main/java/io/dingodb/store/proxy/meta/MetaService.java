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
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.meta.Tenant;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.MetaServiceProvider;
import io.dingodb.meta.TableStatistic;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.PartitionService;
import io.dingodb.sdk.common.serial.RecordEncoder;
import io.dingodb.sdk.service.CoordinatorService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.common.IndexParameter;
import io.dingodb.sdk.service.entity.common.IndexType;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.RawEngine;
import io.dingodb.sdk.service.entity.common.Region;
import io.dingodb.sdk.service.entity.common.RegionDefinition;
import io.dingodb.sdk.service.entity.common.RegionType;
import io.dingodb.sdk.service.entity.coordinator.CreateIdsRequest;
import io.dingodb.sdk.service.entity.coordinator.CreateRegionRequest;
import io.dingodb.sdk.service.entity.coordinator.GetRegionMapRequest;
import io.dingodb.sdk.service.entity.coordinator.IdEpochType;
import io.dingodb.sdk.service.entity.coordinator.QueryRegionRequest;
import io.dingodb.sdk.service.entity.coordinator.RegionCmd.RequestNest.SplitRequest;
import io.dingodb.sdk.service.entity.coordinator.SplitRegionRequest;
import io.dingodb.sdk.service.entity.meta.AddIndexOnTableRequest;
import io.dingodb.sdk.service.entity.meta.CreateSchemaRequest;
import io.dingodb.sdk.service.entity.meta.CreateTablesRequest;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.DropIndexOnTableRequest;
import io.dingodb.sdk.service.entity.meta.DropSchemaRequest;
import io.dingodb.sdk.service.entity.meta.DropTablesRequest;
import io.dingodb.sdk.service.entity.meta.EntityType;
import io.dingodb.sdk.service.entity.meta.GenerateTableIdsRequest;
import io.dingodb.sdk.service.entity.meta.GetSchemasRequest;
import io.dingodb.sdk.service.entity.meta.GetSchemasResponse;
import io.dingodb.sdk.service.entity.meta.GetTableByNameRequest;
import io.dingodb.sdk.service.entity.meta.GetTableMetricsRequest;
import io.dingodb.sdk.service.entity.meta.GetTableMetricsResponse;
import io.dingodb.sdk.service.entity.meta.GetTableRequest;
import io.dingodb.sdk.service.entity.meta.GetTablesRequest;
import io.dingodb.sdk.service.entity.meta.Partition;
import io.dingodb.sdk.service.entity.meta.ReservedSchemaIds;
import io.dingodb.sdk.service.entity.meta.Schema;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.sdk.service.entity.meta.TableIdWithPartIds;
import io.dingodb.sdk.service.entity.meta.TableMetrics;
import io.dingodb.sdk.service.entity.meta.TableMetricsWithId;
import io.dingodb.sdk.service.entity.meta.TableWithPartCount;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.store.proxy.service.AutoIncrementService;
import io.dingodb.store.proxy.service.CodecService;
import io.dingodb.store.service.InfoSchemaService;
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
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.dingodb.common.CommonId.CommonType.TABLE;
import static io.dingodb.partition.DingoPartitionServiceProvider.HASH_FUNC_NAME;
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

    @AutoService(MetaServiceProvider.class)
    public static class Provider implements MetaServiceProvider {
        @Override
        public io.dingodb.meta.MetaService root() {
            return ROOT;
        }
    }

    private static final Pattern pattern = Pattern.compile("^[A-Z_][A-Z\\d_]*$");
    private static final Pattern warnPattern = Pattern.compile(".*[a-z]+.*");

    public final DingoCommonId id;
    public final String name;
    public final io.dingodb.sdk.service.MetaService service;
    public final TsoService tsoService = TsoService.getDefault();
    public final MetaCache cache;
    public final MetaServiceApiImpl api = MetaServiceApiImpl.INSTANCE;
    public final InfoSchemaService infoSchemaService = new InfoSchemaService(Configuration.coordinators());

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

    protected MetaService(DingoCommonId id, String name, io.dingodb.sdk.service.MetaService service, MetaCache cache) {
        this.service = service;
        this.id = id;
        this.name = name;
        this.cache = cache;
    }

    @Override
    public void close() {
        api.close();
        cache.clear();
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
            LogUtils.warn(log, "{} name currently only supports uppercase letters, LowerCase -> UpperCase", source);
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
    public synchronized void createSubMetaService(String name) {
        if (!MetaServiceApiImpl.INSTANCE.isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        if (id != ROOT_SCHEMA_ID) {
            throw new UnsupportedOperationException();
        }
        name = cleanSchemaName(name);
        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
        Long schemaId = coordinatorService.createIds(
                tso(),
                CreateIdsRequest.builder().idEpochType(IdEpochType.ID_NEXT_SCHEMA).count(1).build())
            .getIds().get(0);
        // TODO tenant id
        long tenantId = 0;
        infoSchemaService.createSchema(
            tenantId,
            schemaId,
            SchemaInfo
                .builder()
                .tenantId(tenantId)
                .schemaId(schemaId)
                .name(name)
                .schemaState(SchemaState.PUBLIC)
                .build()
        );
        cache.invalidateMetaServices();
    }

    @Override
    public Map<String, MetaService> getSubMetaServices() {
        if (id != ROOT_SCHEMA_ID) {
            return Collections.emptyMap();
        }
        return cache.getMetaServices();
    }

    @Override
    public synchronized MetaService getSubMetaService(String name) {
        if (id != ROOT_SCHEMA_ID) {
            return null;
        }
        name = cleanSchemaName(name);
        return getSubMetaServices().get(name);
    }

    @Override
    public boolean dropSubMetaService(String name) {
        if (!MetaServiceApiImpl.INSTANCE.isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        if (id != ROOT_SCHEMA_ID) {
            throw new UnsupportedOperationException();
        }
        name = cleanSchemaName(name);
        MetaService metaService = getSubMetaService(name);
        if (metaService == null) {
            return false;
        }
        // TODO tenant id
        long tenantId = 0;
        infoSchemaService.dropSchema(tenantId, metaService.id.getEntityId());
        cache.invalidateMetaServices();
        return true;
    }

    @Override
    public void createTables(
        @NonNull TableDefinition tableDefinition, @NonNull List<TableDefinition> indexTableDefinitions
    ) {
        if (!MetaServiceApiImpl.INSTANCE.isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());

        String tableName = cleanTableName(tableDefinition.getName());
        indexTableDefinitions.forEach($ -> cleanTableName($.getName()));
        // Generate new table ids.
        Long tableEntityId = coordinatorService.createIds(
            tso(),
            CreateIdsRequest.builder()
                .idEpochType(IdEpochType.ID_NEXT_TABLE).count(1)
                .build()
        ).getIds().get(0);
        DingoCommonId tableId = DingoCommonId.builder()
            .entityType(EntityType.ENTITY_TYPE_TABLE)
            .parentEntityId(id.getEntityId())
            .entityId(tableEntityId).build();
        List<DingoCommonId> tablePartIds = coordinatorService.createIds(tso(), CreateIdsRequest.builder()
                .idEpochType(IdEpochType.ID_NEXT_TABLE)
                .count(tableDefinition.getPartDefinition().getDetails().get(0).getOperand().length == 0 ? 1 : 0)
                .build()
            )
            .getIds()
            .stream()
            .map(id -> DingoCommonId.builder()
                .entityType(EntityType.ENTITY_TYPE_PART)
                .parentEntityId(tableEntityId)
                .entityId(id).build())
            .collect(Collectors.toList());
        TableIdWithPartIds tableIdWithPartIds =
            TableIdWithPartIds.builder().tableId(tableId).partIds(tablePartIds).build();
        TableDefinitionWithId tableDefinitionWithId = MAPPER.tableTo(tableIdWithPartIds, tableDefinition);
        // create table
        infoSchemaService.createTableOrView(
            tableDefinitionWithId.getTenantId(),
            id.getEntityId(),
            tableDefinitionWithId.getTableId().getEntityId(),
            tableDefinitionWithId
        );

        // table region
        io.dingodb.sdk.service.entity.meta.TableDefinition withIdTableDefinition = tableDefinitionWithId.getTableDefinition();
        for (Partition partition : withIdTableDefinition.getTablePartition().getPartitions()) {
            CreateRegionRequest request = CreateRegionRequest
                .builder()
                .regionName("T_" + id.getEntityId() + "_" + withIdTableDefinition.getName() + "_part_" + partition.getId().getEntityId())
                .regionType(RegionType.STORE_REGION)
                .replicaNum(withIdTableDefinition.getReplica())
                .range(partition.getRange())
                .rawEngine(RawEngine.RAW_ENG_ROCKSDB)
                .storeEngine(withIdTableDefinition.getStoreEngine())
                .schemaId(id.getEntityId())
                .tableId(tableDefinitionWithId.getTableId().getEntityId())
                .partId(partition.getId().getEntityId())
                .tenantId(tableDefinitionWithId.getTenantId())
                .build();
            coordinatorService.createRegion(tso(), request);
        }

        // create index id
        if (!indexTableDefinitions.isEmpty()) {
            List<DingoCommonId> indexIds = coordinatorService.createIds(
                    tso(),
                    CreateIdsRequest.builder()
                        .idEpochType(IdEpochType.ID_NEXT_TABLE)
                        .count(indexTableDefinitions.size())
                        .build())
                .getIds()
                .stream()
                .map(id -> DingoCommonId.builder()
                    .entityType(EntityType.ENTITY_TYPE_INDEX)
                    .parentEntityId(tableEntityId)
                    .entityId(id)
                    .build())
                .collect(Collectors.toList());
            List<TableDefinitionWithId> indexWithIds = new ArrayList<>();
            for (int i = 0; i < indexTableDefinitions.size(); i++) {
                int finalI = i;
                Integer count = 0;
                for (PartitionDetailDefinition detail : indexTableDefinitions.get(i).getPartDefinition().getDetails()) {
                    count += detail.getOperand().length == 0 ? 1 : detail.getOperand().length;
                }
                List<DingoCommonId> indexPartIds = coordinatorService.createIds(
                        tso(),
                        CreateIdsRequest.builder()
                            .idEpochType(IdEpochType.ID_NEXT_TABLE)
                            .count(count == 0 ? 1 : count)
                            .build())
                    .getIds()
                    .stream()
                    .map(id -> DingoCommonId.builder()
                        .entityType(EntityType.ENTITY_TYPE_PART)
                        .parentEntityId(indexIds.get(finalI).getEntityId())
                        .entityId(id).build())
                    .collect(Collectors.toList());
                TableIdWithPartIds indexIdWithPartIds = TableIdWithPartIds.builder()
                    .tableId(indexIds.get(i))
                    .partIds(indexPartIds)
                    .build();
                TableDefinitionWithId indexWithId = Stream.of(indexTableDefinitions.get(i))
                    .map(index -> MAPPER.tableTo(indexIdWithPartIds, index))
                    .peek(td -> MAPPER.resetIndexParameter(td.getTableDefinition()))
                    .peek(td -> td.getTableDefinition().setName(tableName + "." + td.getTableDefinition().getName()))
                    .findAny()
                    .get();
                indexWithIds.add(indexWithId);
                // create index
                infoSchemaService.createIndex(
                    indexWithId.getTenantId(),
                    id.getEntityId(),
                    tableEntityId,
                    indexWithId
                );
            }

            // index region
            for (TableDefinitionWithId withId : indexWithIds) {
                io.dingodb.sdk.service.entity.meta.TableDefinition definition = withId.getTableDefinition();
                for (Partition partition : definition.getTablePartition().getPartitions()) {
                    IndexParameter indexParameter = definition.getIndexParameter();
                    if (indexParameter.getVectorIndexParameter() != null) {
                        indexParameter.setIndexType(IndexType.INDEX_TYPE_VECTOR);
                    } else if (indexParameter.getDocumentIndexParameter() != null) {
                        indexParameter.setIndexType(IndexType.INDEX_TYPE_DOCUMENT);
                    }
                    CreateRegionRequest request = CreateRegionRequest
                        .builder()
                        .regionName("I_" + id.getEntityId() + "_" + definition.getName() + "_part_" + partition.getId().getEntityId())
                        .regionType(definition.getIndexParameter().getIndexType() == IndexType.INDEX_TYPE_SCALAR ?
                            RegionType.STORE_REGION : RegionType.INDEX_REGION)
                        .replicaNum(definition.getReplica())
                        .range(partition.getRange())
                        .rawEngine(RawEngine.RAW_ENG_ROCKSDB)
                        .storeEngine(definition.getStoreEngine())
                        .schemaId(id.getEntityId())
                        .tableId(tableId.getEntityId())
                        .partId(partition.getId().getEntityId())
                        .tenantId(withId.getTenantId())
                        .indexId(withId.getTableId().getEntityId())
                        .indexParameter(indexParameter)
                        .build();
                    coordinatorService.createRegion(tso(), request);
                }
            }
        }

        cache.refreshSchema(name);
    }

    @Override
    public void createIndex(CommonId tableId, TableDefinition table, TableDefinition index) {
        if (!MetaServiceApiImpl.INSTANCE.isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        String tableName = cleanTableName(table.getName());
        TableIdWithPartIds tableIdWithPartIds = service.generateTableIds(
            tso(),
            GenerateTableIdsRequest.builder()
                .schemaId(id)
                .count(
                TableWithPartCount.builder()
                    .indexCount(1)
                    .indexPartCount(Collections.singletonList(index.getPartDefinition().getDetails().size()))
                    .build()
            ).build()).getIds().get(0);
        TableDefinitionWithId tableDefinitionWithId = Stream.of(index).map(i -> MAPPER.tableTo(tableIdWithPartIds, i))
            .peek(td -> MAPPER.resetIndexParameter(td.getTableDefinition()))
            .peek(td -> td.getTableDefinition().setName(tableName + "." + td.getTableDefinition().getName()))
            .findAny().get();

        api.addIndexOnTable(
            tso(),
            name,
            AddIndexOnTableRequest.builder()
                .tableId(MAPPER.idTo(tableId))
                .tableDefinitionWithId(tableDefinitionWithId)
                .build());
        cache.invalidateTable(id.getEntityId(), tableId.seq);
    }

    @Override
    public void createDifferenceIndex(CommonId tableId, CommonId indexId, IndexTable indexTable) {
        dropIndex(tableId, indexId);

        TableIdWithPartIds tableIdWithPartIds = service.generateTableIds(
            tso(),
            GenerateTableIdsRequest.builder().count(
                TableWithPartCount.builder()
                    .indexCount(1)
                    .indexPartCount(Collections.singletonList(indexTable.getPartitions().size()))
                    .build()
            ).build()).getIds().get(0);

        /*api.addIndexOnTable(tso(),
            name,
            AddIndexOnTableRequest.builder()
                .tableId(MAPPER.idTo(tableId))
                .tableDefinitionWithId(TableDefinitionWithId.builder()
                    .tableId(tableIdWithPartIds.getTableId())
                    .tableDefinition(indexTable)));*/

    }

    @Override
    public void updateTable(CommonId tableId, @NonNull Table table) {
        io.dingodb.sdk.service.entity.meta.TableDefinition oldTable = service.getTable(
            tso(),
            GetTableRequest.builder().tableId(MAPPER.idTo(tableId)).build()
        ).getTableDefinitionWithId().getTableDefinition();

        /*service.updateTables(
            tso(),
            UpdateTablesRequest.builder()
                .tableDefinitionWithId(TableDefinitionWithId.builder()
                    .tableId(MAPPER.idTo(tableId))
                    .tableDefinition()
                    .build())
                .build())*/
    }

    @Override
    public void dropIndex(CommonId table, CommonId index) {
        api.dropIndexOnTable(
            tso(),
            name,
            DropIndexOnTableRequest.builder()
                .tableId(MAPPER.idTo(table))
                .indexId(MAPPER.idTo(index))
                .build()
        );
    }

    @Override
    public Map<CommonId, TableDefinition> getTableIndexDefinitions(@NonNull CommonId id) {
        // infoSchemaService.getTable()

        return service.getTables(
            tso(),
            GetTablesRequest.builder()
                .tableId(MAPPER.idTo(id))
                .build()
        ).getTableDefinitionWithIds().stream().collect(Collectors.toMap(entry -> MAPPER.idFrom(entry.getTableId()), entry -> {
            TableDefinition table = mapping1(entry);
            String tableName = table.getName();
            String[] split = tableName.split("\\.");
                if (split.length > 1) {
                    tableName = split[split.length - 1];
                }
                return table.copyWithName(tableName);
            }));
    }

    private TableDefinition mapping1(TableDefinitionWithId tableDefinitionWithId) {
        io.dingodb.sdk.service.entity.meta.TableDefinition table = tableDefinitionWithId.getTableDefinition();
        Map<String, String> properties = table.getProperties();
        Properties prop = new Properties();
        if (properties != null) {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                prop.setProperty(entry.getKey(), entry.getValue());
            }
        }
        return TableDefinition.builder()
            .name(table.getName())
            .columns(table.getColumns().stream().map(this::mapping1).collect(Collectors.toList()))
            .collate(table.getCollate())
            .charset(table.getCharset())
            .comment(table.getComment())
            .version(table.getVersion())
            .createTime(table.getCreateTimestamp())
            .ttl((int) table.getTtl())
            .rowFormat(table.getRowFormat())
            .tableType(table.getTableType())
            .updateTime(table.getUpdateTimestamp())
            // .partDefinition()
            .engine(table.getEngine().name())
            .replica(table.getReplica())
            .autoIncrement(table.getAutoIncrement())
            .createSql(table.getCreateSql())
            .properties(prop)
            .build();
    }

    private ColumnDefinition mapping1(io.dingodb.sdk.service.entity.meta.ColumnDefinition column) {
        return ColumnDefinition.builder()
            .name(column.getName())
            .type(column.getSqlType())
            .scale(column.getScale())
            .primary(column.getIndexOfKey())
            .state(column.getState())
            .nullable(column.isNullable())
            .createVersion(column.getCreateVersion())
            .defaultValue(column.getDefaultVal())
            .precision(column.getPrecision())
            .elementType(column.getElementType())
            .comment(column.getComment())
            .autoIncrement(column.isAutoIncrement())
            .updateVersion(column.getUpdateVersion())
            .deleteVersion(column.getDeleteVersion())
            .build();
    }

    @Override
    public boolean truncateTable(@NonNull String tableName) {
        if (!MetaServiceApiImpl.INSTANCE.isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        // Get old table and indexes
        long tenantId = 0;
        TableDefinitionWithId table = Optional.mapOrGet(infoSchemaService.getTable(tenantId, id.getEntityId(), tableName), __ -> (TableDefinitionWithId) __, () -> null);

        List<Object> indexList = infoSchemaService.listIndex(tenantId, id.getEntityId(), table.getTableId().getEntityId());
        List<TableDefinitionWithId> indexes = indexList.stream().map(object -> (TableDefinitionWithId) object).collect(Collectors.toList());

        // Generate new table ids.
        io.dingodb.sdk.service.entity.meta.TableDefinition tableDefinition = table.getTableDefinition();
        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
        Long tableEntityId = coordinatorService.createIds(
            tso(),
            CreateIdsRequest.builder()
                .idEpochType(IdEpochType.ID_NEXT_TABLE).count(1)
                .build()).getIds().get(0);
        DingoCommonId tableId = DingoCommonId.builder()
            .entityType(EntityType.ENTITY_TYPE_TABLE)
            .parentEntityId(id.getEntityId())
            .entityId(tableEntityId)
            .build();
        List<DingoCommonId> tablePartIds = coordinatorService.createIds(tso(), CreateIdsRequest.builder()
                .idEpochType(IdEpochType.ID_NEXT_TABLE)
                .count(tableDefinition.getTablePartition().getPartitions().size())
                .build()
            ).getIds().stream()
            .map(id -> DingoCommonId.builder()
                .entityType(EntityType.ENTITY_TYPE_PART)
                .parentEntityId(tableEntityId)
                .entityId(id).build())
            .collect(Collectors.toList());

        List<DingoCommonId> oldIds = new ArrayList<>();
        oldIds.add(table.getTableId());
        indexes.stream().map(TableDefinitionWithId::getTableId).forEach(oldIds::add);

        // Reset table id.
        TableIdWithPartIds newTableId =
            TableIdWithPartIds.builder().tableId(tableId).partIds(tablePartIds).build();
        oldIds.forEach(id -> infoSchemaService.dropTable(tenantId, id.getEntityId(), id.getEntityId()));
        resetTableId(newTableId, table);

        cache.refreshSchema(name);
        // create table、table region
        infoSchemaService.createTableOrView(tenantId, id.getEntityId(), table.getTableId().getEntityId(), table);
        for (Partition partition : tableDefinition.getTablePartition().getPartitions()) {
            CreateRegionRequest request = CreateRegionRequest.builder()
                .regionName("T_" + id.getEntityId() + "_" + tableDefinition.getName() + "_part_" + partition.getId().getEntityId())
                .regionType(RegionType.STORE_REGION)
                .replicaNum(tableDefinition.getReplica())
                .range(partition.getRange())
                .rawEngine(RawEngine.RAW_ENG_ROCKSDB)
                .storeEngine(tableDefinition.getStoreEngine())
                .schemaId(id.getEntityId())
                .tableId(table.getTableId().getEntityId())
                .partId(partition.getId().getEntityId())
                .tenantId(table.getTenantId())
                .build();
            coordinatorService.createRegion(tso(), request);
        }

        // create index id
        if (!indexes.isEmpty()) {
            List<DingoCommonId> indexOldIds = indexes.stream().map(TableDefinitionWithId::getTableId).collect(Collectors.toList());
            indexOldIds.forEach(oid -> infoSchemaService.dropTable(tenantId, id.getEntityId(), oid.getEntityId()));
            List<DingoCommonId> indexIds = coordinatorService.createIds(
                    tso(),
                    CreateIdsRequest.builder().idEpochType(IdEpochType.ID_NEXT_TABLE).count(indexes.size()).build()
                ).getIds()
                .stream()
                .map(id -> DingoCommonId.builder()
                    .entityId(id)
                    .entityType(EntityType.ENTITY_TYPE_INDEX)
                    .parentEntityId(tableEntityId)
                    .build())
                .collect(Collectors.toList());

            for (int i = 0; i < indexes.size(); i++) {
                int finalI = i;
                TableDefinitionWithId indexDefinitionWithId = indexes.get(i);
                List<DingoCommonId> indexPartIds = coordinatorService.createIds(
                        tso(), CreateIdsRequest.builder()
                            .idEpochType(IdEpochType.ID_NEXT_TABLE)
                            .count(indexDefinitionWithId.getTableDefinition().getTablePartition().getPartitions().size())
                            .build()
                    ).getIds().stream()
                    .map(id -> DingoCommonId.builder()
                        .entityType(EntityType.ENTITY_TYPE_PART)
                        .parentEntityId(indexIds.get(finalI).getEntityId())
                        .entityId(id)
                        .build())
                    .collect(Collectors.toList());
                TableIdWithPartIds indexIdWithPartIds = TableIdWithPartIds.builder()
                    .tableId(indexIds.get(i))
                    .partIds(indexPartIds)
                    .build();

                infoSchemaService.createIndex(
                    indexDefinitionWithId.getTenantId(),
                    id.getEntityId(),
                    tableEntityId,
                    indexDefinitionWithId
                );
                // reset indexes id.
                resetTableId(indexIdWithPartIds, indexDefinitionWithId);
            }
            for (TableDefinitionWithId withId : indexes) {
                io.dingodb.sdk.service.entity.meta.TableDefinition definition = withId.getTableDefinition();
                for (Partition partition : definition.getTablePartition().getPartitions()) {
                    IndexParameter indexParameter = definition.getIndexParameter();
                    if (indexParameter.getVectorIndexParameter() != null) {
                        indexParameter.setIndexType(IndexType.INDEX_TYPE_VECTOR);
                    } else if (indexParameter.getDocumentIndexParameter() != null) {
                        indexParameter.setIndexType(IndexType.INDEX_TYPE_DOCUMENT);
                    }
                    CreateRegionRequest request = CreateRegionRequest.builder()
                        .regionName("I_" + id.getEntityId() + "_" + definition.getName() + "_part_" + partition.getId().getEntityId())
                        .regionType(definition.getIndexParameter().getIndexType() == IndexType.INDEX_TYPE_SCALAR ?
                            RegionType.STORE_REGION : RegionType.INDEX_REGION)
                        .replicaNum(definition.getReplica())
                        .range(partition.getRange())
                        .rawEngine(RawEngine.RAW_ENG_ROCKSDB)
                        .storeEngine(definition.getStoreEngine())
                        .schemaId(id.getEntityId())
                        .tableId(tableId.getEntityId())
                        .partId(partition.getId().getEntityId())
                        .tenantId(withId.getTenantId())
                        .indexId(withId.getTableId().getEntityId())
                        .indexParameter(indexParameter)
                        .build();
                    coordinatorService.createRegion(tso(), request);
                }
            }
        }

        cache.refreshSchema(name);
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
        if (!MetaServiceApiImpl.INSTANCE.isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        Table table = cache.getTable(name, tableName);
        if (table == null) {
            return false;
        }
        List<CommonId> indexIds = table.getIndexes().stream().map(Table::getTableId).collect(Collectors.toList());
        List<CommonId> tableIds = Stream.concat(
            Stream.of(table.getTableId()), indexIds.stream()).collect(Collectors.toList()
        );
        // api.dropTables(tso(), name, tableName, DropTablesRequest.builder().tableIds(MAPPER.idTo(tableIds)).build());
        long tenantId = 0;
        tableIds.forEach(tableId -> infoSchemaService.dropTable(tenantId, id.getEntityId(), tableId.seq));
        cache.refreshSchema(name);
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
        return cache.getTables(name);
    }

    @Override
    public NavigableMap<ComparableByteArray, RangeDistribution> getRangeDistribution(CommonId id) {
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
        GetSchemasResponse getSchemasResponse
            = service.getSchemas(tso, GetSchemasRequest.builder().schemaId(io.dingodb.store.proxy.meta.MetaService.ROOT.id).build());
        List<Long> tableIds = getSchemasResponse
            .getSchemas().stream()
            .map(Schema::getTableIds)
            .filter(Objects::nonNull)
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
    public void addDistribution(String tableName, PartitionDetailDefinition detail) {
        tableName = cleanTableName(tableName);
        Table table = getTable(tableName);
        if (table == null) {
            throw new RuntimeException("Table not found.");
        }
        PartitionService partitionService = PartitionService.getService(table.partitionStrategy);
        TupleType keyType = (TupleType) table.onlyKeyType();
        TupleMapping keyMapping = TupleMapping.of(IntStream.range(0, keyType.fieldCount()).toArray());
        RecordEncoder encoder = new RecordEncoder(
            1, CodecService.createSchemasForType(keyType, keyMapping), 0
        );
        byte[] key = encoder.encodeKeyPrefix(detail.getOperand(), detail.getOperand().length);
        CommonId commonId = partitionService.calcPartId(key, getRangeDistribution(table.tableId));
        encoder.resetKeyPrefix(key, commonId.domain);
        if (table.getEngine().startsWith("TXN")) {
            key[0] = 't';
        }
        Services.coordinatorService(Configuration.coordinatorSet()).splitRegion(
            tso(),
            SplitRegionRequest.builder()
                .splitRequest(SplitRequest.builder().splitFromRegionId(commonId.seq).splitWatershedKey(key).build())
                .build()
        );
        ComparableByteArray comparableKey = new ComparableByteArray(
            key, Objects.equals(table.partitionStrategy, HASH_FUNC_NAME) ? 0 : 9
        );

        Utils.loop(() -> !checkSplitFinish(comparableKey, table), TimeUnit.SECONDS.toNanos(1), 60);
        if (checkSplitFinish(comparableKey, table)) {
            return;
        }
        LogUtils.warn(log, "Add distribution wait timeout, refresh distributions run in the background.");
        Executors.execute("wait-split", () -> {
            Utils.loop(() -> !checkSplitFinish(comparableKey, table), TimeUnit.SECONDS.toNanos(1));
        });
    }

    private boolean checkSplitFinish(ComparableByteArray comparableKey, Table table) {
        return comparableKey.compareTo(cache.getRangeDistribution(table.tableId).floorKey(comparableKey)) == 0;
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
                return 0D;
            }
        };
    }

    @Override
    public TableStatistic getTableStatistic(@NonNull CommonId tableId) {
        return new TableStatistic() {

            @Override
            public byte[] getMinKey() {
                return Optional.ofNullable(service.getTableMetrics(
                    tso(), GetTableMetricsRequest.builder().tableId(MAPPER.idTo(tableId)).build()
                )).map(GetTableMetricsResponse::getTableMetrics)
                    .map(TableMetricsWithId::getTableMetrics)
                    .map(TableMetrics::getMinKey)
                    .orNull();
            }

            @Override
            public byte[] getMaxKey() {
                return Optional.ofNullable(service.getTableMetrics(
                        tso(), GetTableMetricsRequest.builder().tableId(MAPPER.idTo(tableId)).build()
                    )).map(GetTableMetricsResponse::getTableMetrics)
                    .map(TableMetricsWithId::getTableMetrics)
                    .map(TableMetrics::getMaxKey)
                    .orNull();
            }

            @Override
            public long getPartCount() {
                return Optional.ofNullable(service.getTableMetrics(
                        tso(), GetTableMetricsRequest.builder().tableId(MAPPER.idTo(tableId)).build()
                    )).map(GetTableMetricsResponse::getTableMetrics)
                    .map(TableMetricsWithId::getTableMetrics)
                    .map(TableMetrics::getPartCount)
                    .orElse(0);
            }

            @Override
            public Double getRowCount() {
                return 0D;
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

    @Override
    public long getLastId(CommonId tableId) {
        return AutoIncrementService.INSTANCE.getLastId(tableId);
    }
}
