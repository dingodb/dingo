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

import com.codahale.metrics.Timer;
import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.ddl.GcDeleteRegion;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.meta.Tenant;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.mysql.DingoErrUtil;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.IndexDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.tenant.TenantConstant;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.DefinitionUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.MetaServiceProvider;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.sdk.common.serial.RecordEncoder;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.sdk.service.CoordinatorService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.common.Engine;
import io.dingodb.sdk.service.entity.common.IndexParameter;
import io.dingodb.sdk.service.entity.common.IndexType;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.Range;
import io.dingodb.sdk.service.entity.common.RawEngine;
import io.dingodb.sdk.service.entity.common.Region;
import io.dingodb.sdk.service.entity.common.RegionDefinition;
import io.dingodb.sdk.service.entity.common.RegionType;
import io.dingodb.sdk.service.entity.coordinator.CreateIdsRequest;
import io.dingodb.sdk.service.entity.coordinator.CreateRegionRequest;
import io.dingodb.sdk.service.entity.coordinator.DropRegionRequest;
import io.dingodb.sdk.service.entity.coordinator.GetRegionMapRequest;
import io.dingodb.sdk.service.entity.coordinator.IdEpochType;
import io.dingodb.sdk.service.entity.coordinator.QueryRegionRequest;
import io.dingodb.sdk.service.entity.coordinator.RegionCmd.RequestNest.SplitRequest;
import io.dingodb.sdk.service.entity.coordinator.SplitRegionRequest;
import io.dingodb.sdk.service.entity.meta.CreateAutoIncrementRequest;
import io.dingodb.sdk.service.entity.meta.CreateTenantRequest;
import io.dingodb.sdk.service.entity.meta.DeleteAutoIncrementRequest;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.DropTenantRequest;
import io.dingodb.sdk.service.entity.meta.EntityType;
import io.dingodb.sdk.service.entity.meta.GetSchemasRequest;
import io.dingodb.sdk.service.entity.meta.GetSchemasResponse;
import io.dingodb.sdk.service.entity.meta.Partition;
import io.dingodb.sdk.service.entity.meta.ReservedSchemaIds;
import io.dingodb.sdk.service.entity.meta.Schema;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.sdk.service.entity.meta.TableIdWithPartIds;
import io.dingodb.sdk.service.entity.meta.UpdateTenantRequest;
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
import java.util.HashSet;
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
import static io.dingodb.common.mysql.error.ErrorCode.ErrUnknown;
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

        @Override
        public io.dingodb.meta.MetaService snapshot(long ts) {
            return new MetaService(ts);
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
    public final InfoSchemaService infoSchemaService = new InfoSchemaService();

    public MetaService() {
        Set<Location> coordinators = Configuration.coordinatorSet();
        this.service = Services.metaService(coordinators);
        this.id = ROOT_SCHEMA_ID;
        this.name = ROOT_NAME;
        this.cache = new MetaCache(coordinators);
    }

    public MetaService(long pointTs) {
        Set<Location> coordinators = Configuration.coordinatorSet();
        this.service = Services.metaService(coordinators);
        this.id = ROOT_SCHEMA_ID;
        this.name = ROOT_NAME;
        this.cache = new MetaCache(coordinators, pointTs);
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

    private static String cleanTableName(String name) {
        return cleanName(name, "Table");
    }

    private static String cleanSchemaName(String name) {
        return cleanName(name, "Schema");
    }

    private static String cleanName(String name, String source) {
        if (warnPattern.matcher(name).matches()) {
            LogUtils.warn(log, "{} name currently only supports uppercase letters, LowerCase -> UpperCase",
                source);
            name = name.toUpperCase();
        }
        if (!pattern.matcher(name).matches()) {
            throw new RuntimeException(source + " name currently only supports uppercase letters, "
                + "digits, and underscores");
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
        return tsoService.cacheTso();
    }

    @Override
    public synchronized void createSubMetaService(String name) {
        if (id != ROOT_SCHEMA_ID) {
            throw new UnsupportedOperationException();
        }
        name = cleanSchemaName(name);
        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
        Long schemaId = coordinatorService.createIds(
                tso(),
                CreateIdsRequest.builder().idEpochType(IdEpochType.ID_NEXT_SCHEMA).count(1).build())
            .getIds().get(0);
        infoSchemaService.createSchema(
            schemaId,
            SchemaInfo
                .builder()
                .schemaId(schemaId)
                .name(name)
                .schemaState(SchemaState.SCHEMA_PUBLIC)
                .build()
        );
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
        if (id != ROOT_SCHEMA_ID) {
            throw new UnsupportedOperationException();
        }
        name = cleanSchemaName(name);
        MetaService metaService = getSubMetaService(name);
        if (metaService == null) {
            return false;
        }
        infoSchemaService.dropSchema(metaService.id.getEntityId());
        return true;
    }

    @Override
    public void createView(long schemaId, String tableName, TableDefinition tableDefinition) {
        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());

        long tableEntityId;
        if (tableDefinition.getPrepareTableId() != 0) {
            tableEntityId = tableDefinition.getPrepareTableId();
        } else {
            // Generate new table ids.
            tableEntityId = coordinatorService.createIds(
                tso(),
                CreateIdsRequest.builder()
                    .idEpochType(IdEpochType.ID_NEXT_TABLE).count(1)
                    .build()
            ).getIds().get(0);
        }
        DingoCommonId tableId = DingoCommonId.builder()
            .entityType(EntityType.ENTITY_TYPE_TABLE)
            .parentEntityId(schemaId)
            .entityId(tableEntityId).build();
        TableIdWithPartIds tableIdWithPartIds =
            TableIdWithPartIds.builder().tableId(tableId).build();
        TableDefinitionWithId tableDefinitionWithId = MAPPER.tableTo(
            tableIdWithPartIds, tableDefinition, TenantConstant.TENANT_ID
        );
        // create view
        infoSchemaService.createTableOrView(
            schemaId,
            tableDefinitionWithId.getTableId().getEntityId(),
            tableDefinitionWithId
        );
    }


    @Override
    public long createTables(
        @NonNull TableDefinition tableDefinition, @NonNull List<IndexDefinition> indexTableDefinitions
    ) {
        return createTables(id.getEntityId(), tableDefinition, indexTableDefinitions);
    }

    @Override
    public long createTables(
        long schemaId, @NonNull TableDefinition tableDefinition, @NonNull List<IndexDefinition> indexTableDefinitions
    ) {
        validatePartBy(tableDefinition);
        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());

        String tableName = cleanTableName(tableDefinition.getName());
        indexTableDefinitions.forEach($ -> cleanTableName($.getName()));
        long tableEntityId;
        if (tableDefinition.getPrepareTableId() != 0) {
            tableEntityId = tableDefinition.getPrepareTableId();
        } else {
            // Generate new table ids.
            tableEntityId = coordinatorService.createIds(
                tso(),
                CreateIdsRequest.builder()
                    .idEpochType(IdEpochType.ID_NEXT_TABLE).count(1)
                    .build()
            ).getIds().get(0);
        }
        DingoCommonId tableId = DingoCommonId.builder()
            .entityType(EntityType.ENTITY_TYPE_TABLE)
            .parentEntityId(schemaId)
            .entityId(tableEntityId).build();
        List<DingoCommonId> tablePartIds = coordinatorService.createIds(tso(), CreateIdsRequest.builder()
                .idEpochType(IdEpochType.ID_NEXT_TABLE)
                .count(tableDefinition.getPartDefinition().getDetails().size())
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
        TableDefinitionWithId tableDefinitionWithId = MAPPER.tableTo(tableIdWithPartIds, tableDefinition,
            TenantConstant.TENANT_ID);

        synchronized (this) {
            io.dingodb.meta.InfoSchemaService service = io.dingodb.meta.InfoSchemaService.root();
            Object tabObj = service.getTable(schemaId, tableName);
            if (tabObj != null) {
                throw new RuntimeException("table has existed");
            }
            // create table
            infoSchemaService.createTableOrView(
                schemaId,
                tableDefinitionWithId.getTableId().getEntityId(),
                tableDefinitionWithId
            );
        }

        // table region
        io.dingodb.sdk.service.entity.meta.TableDefinition withIdTableDefinition
            = tableDefinitionWithId.getTableDefinition();
        for (Partition partition : withIdTableDefinition.getTablePartition().getPartitions()) {
            CreateRegionRequest request = CreateRegionRequest
                .builder()
                .regionName("T_" + schemaId + "_" + withIdTableDefinition.getName()
                    + "_part_" + partition.getId().getEntityId())
                .regionType(RegionType.STORE_REGION)
                .replicaNum(withIdTableDefinition.getReplica())
                .range(partition.getRange())
                .rawEngine(getRawEngine(withIdTableDefinition.getEngine()))
                .storeEngine(withIdTableDefinition.getStoreEngine())
                .schemaId(schemaId)
                .tableId(tableDefinitionWithId.getTableId().getEntityId())
                .partId(partition.getId().getEntityId())
                .tenantId(tableDefinitionWithId.getTenantId())
                .build();
            LogUtils.info(log, "create region range:{}", partition.getRange());
            try {
                coordinatorService.createRegion(tso(), request);
            } catch (Exception e) {
                LogUtils.error(log, "create region error, range:{}", partition.getRange());
                throw e;
            }
        }
        long incrementColCount = tableDefinition.getColumns()
            .stream()
            .filter(ColumnDefinition::isAutoIncrement)
            .count();
        if (incrementColCount > 0) {
            io.dingodb.sdk.service.MetaService metaService
                = Services.autoIncrementMetaService(Configuration.coordinatorSet());
            metaService.createAutoIncrement(
                tso(), CreateAutoIncrementRequest.builder()
                    .tableId(tableId)
                    .startId(tableDefinition.getAutoIncrement())
                    .build()
            );
        }
        if (indexTableDefinitions.isEmpty()) {
            return tableEntityId;
        }

        // create index id
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
            int count = 0;
            TableDefinition indexDef = indexTableDefinitions.get(i);
            validatePartBy(indexDef);
            for (PartitionDetailDefinition detail : indexDef.getPartDefinition().getDetails()) {
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
                .map(index -> {
                    TableDefinitionWithId td = MAPPER.tableTo(
                        indexIdWithPartIds, index, TenantConstant.TENANT_ID
                    );
                    MAPPER.resetIndexParameter(td.getTableDefinition(), index);
                    return td;
                })
                .peek(td -> td.getTableDefinition().setName(
                    tableName + "." + td.getTableDefinition().getName())
                )
                .findAny()
                .get();
            indexWithIds.add(indexWithId);
            // create index
            infoSchemaService.createIndex(
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
                    .regionName("I_" + schemaId + "_" + definition.getName() + "_part_"
                        + partition.getId().getEntityId())
                    .regionType(mapping(definition.getIndexParameter().getIndexType()))
                    .replicaNum(withId.getTableDefinition().getReplica())
                    .range(partition.getRange())
                    .rawEngine(getRawEngine(definition.getEngine()))
                    .storeEngine(definition.getStoreEngine())
                    .schemaId(schemaId)
                    .tableId(tableId.getEntityId())
                    .partId(partition.getId().getEntityId())
                    .tenantId(withId.getTenantId())
                    .indexId(withId.getTableId().getEntityId())
                    .indexParameter(indexParameter)
                    .build();
                try {
                    LogUtils.info(log, "create index region, range:{}", partition.getRange());
                    coordinatorService.createRegion(tso(), request);
                } catch (Exception e) {
                    LogUtils.error(log, "create index region error, range:{}", partition.getRange());
                    throw e;
                }
            }
        }
        return tableEntityId;
    }

    @Override
    public synchronized void recoverTable(long schemaId, Object tableWithId, List<Object> indexTableList) {
        TableDefinitionWithId tableDefinitionWithId = (TableDefinitionWithId) tableWithId;
        Object tabObj = infoSchemaService.getTable(schemaId, tableDefinitionWithId.getTableDefinition().getName());
        if (tabObj != null) {
            throw new RuntimeException("table has existed");
        }
        // recover table meta
        infoSchemaService.createTableOrView(
            schemaId,
            tableDefinitionWithId.getTableId().getEntityId(),
            tableDefinitionWithId
        );
        // recover index meta
        for (Object indexObj : indexTableList) {
            TableDefinitionWithId indexWithId = (TableDefinitionWithId) indexObj;
            infoSchemaService.createIndex(
                id.getEntityId(),
                tableDefinitionWithId.getTableId().getEntityId(),
                indexWithId
            );
        }
    }

    private static RegionType mapping(IndexType indexType) {
        switch (indexType) {
            case INDEX_TYPE_VECTOR:
                return RegionType.INDEX_REGION;
            case INDEX_TYPE_SCALAR:
                return RegionType.STORE_REGION;
            case INDEX_TYPE_DOCUMENT:
                return RegionType.DOCUMENT_REGION;
            default:
                throw new IllegalStateException("Unexpected value: " + indexType);
        }
    }

    static io.dingodb.sdk.service.entity.meta.Tenant mapping(Tenant tenant) {
        return io.dingodb.sdk.service.entity.meta.Tenant.builder()
            .id(tenant.getId())
            .name(tenant.getName())
            .comment(tenant.getRemarks())
            .createTimestamp(tenant.getCreatedTime())
            .updateTimestamp(tenant.getUpdatedTime())
            .deleteTimestamp(0)
            .build();
    }

    @Override
    public long createReplicaTable(
        long schemaId, Object tableDefinition, String tableName
    ) {
        TableDefinitionWithId tableDefinitionWithId = (TableDefinitionWithId) tableDefinition;
        long originTableId = tableDefinitionWithId.getTableId().getEntityId();
        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
        // Generate new table ids.
        long tableEntityId = coordinatorService.createIds(
            tso(),
            CreateIdsRequest.builder()
                .idEpochType(IdEpochType.ID_NEXT_TABLE).count(1)
                .build()
        ).getIds().get(0);
        DingoCommonId tableId = DingoCommonId.builder()
            .entityType(EntityType.ENTITY_TYPE_INDEX)
            .parentEntityId(originTableId)
            .entityId(tableEntityId).build();
        List<DingoCommonId> tablePartIds = coordinatorService.createIds(tso(), CreateIdsRequest.builder()
                .idEpochType(IdEpochType.ID_NEXT_TABLE)
                .count(tableDefinitionWithId.getTableDefinition().getTablePartition().getPartitions().size())
                .build()
            )
            .getIds()
            .stream()
            .map(id -> DingoCommonId.builder()
                .entityType(EntityType.ENTITY_TYPE_PART)
                .parentEntityId(tableEntityId)
                .entityId(id).build())
            .collect(Collectors.toList());
        tableDefinitionWithId.setTableId(tableId);
        tableDefinitionWithId.getTableDefinition()
            .getTablePartition().getPartitions()
            .forEach(partition -> {
                partition.setId(tablePartIds.remove(0));
                byte[] startKey = MAPPER.realKey(partition.getRange().getStartKey(), partition.getId(), (byte) 't');
                byte[] endKey = MAPPER.nextKey(partition.getId(), (byte) 't');
                partition.getRange().setStartKey(startKey);
                partition.getRange().setEndKey(endKey);
            });
        // create table
        infoSchemaService.createReplicaTable(
            schemaId,
            originTableId,
            tableDefinitionWithId
        );

        // table region
        io.dingodb.sdk.service.entity.meta.TableDefinition withIdTableDefinition
            = tableDefinitionWithId.getTableDefinition();
        for (Partition partition : withIdTableDefinition.getTablePartition().getPartitions()) {
            CreateRegionRequest request = CreateRegionRequest
                .builder()
                .regionName("T_" + schemaId + "_" + tableName + "_part_" + partition.getId().getEntityId())
                .regionType(RegionType.STORE_REGION)
                .replicaNum(withIdTableDefinition.getReplica())
                .range(partition.getRange())
                .rawEngine(getRawEngine(withIdTableDefinition.getEngine()))
                .storeEngine(withIdTableDefinition.getStoreEngine())
                .schemaId(schemaId)
                .tableId(originTableId)
                .indexId(tableEntityId)
                .partId(partition.getId().getEntityId())
                .tenantId(tableDefinitionWithId.getTenantId())
                .build();
            try {
                LogUtils.info(log, "create replicate region, range:{}", partition.getRange());
                coordinatorService.createRegion(tso(), request);
            } catch (Exception e) {
                LogUtils.error(log, "create replicate region error, range:{}", partition.getRange());
                throw e;
            }
        }

        return tableEntityId;
    }

    public void createIndexReplicaTable(
        long schemaId, long originPriTableId, Object indexDefinition, String indexName
    ) {
        TableDefinitionWithId tableDefinitionWithId = (TableDefinitionWithId) indexDefinition;
        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
        // Generate new table ids.
        long tableEntityId = coordinatorService.createIds(
            tso(),
            CreateIdsRequest.builder()
                .idEpochType(IdEpochType.ID_NEXT_TABLE).count(1)
                .build()
        ).getIds().get(0);
        DingoCommonId tableId = DingoCommonId.builder()
            .entityType(EntityType.ENTITY_TYPE_INDEX)
            .parentEntityId(originPriTableId)
            .entityId(tableEntityId).build();
        List<DingoCommonId> tablePartIds = coordinatorService.createIds(tso(), CreateIdsRequest.builder()
                .idEpochType(IdEpochType.ID_NEXT_TABLE)
                .count(tableDefinitionWithId.getTableDefinition().getTablePartition().getPartitions().size())
                .build()
            )
            .getIds()
            .stream()
            .map(id -> DingoCommonId.builder()
                .entityType(EntityType.ENTITY_TYPE_PART)
                .parentEntityId(tableEntityId)
                .entityId(id).build())
            .collect(Collectors.toList());
        tableDefinitionWithId.setTableId(tableId);
        tableDefinitionWithId.getTableDefinition()
            .getTablePartition().getPartitions()
            .forEach(partition -> {
                partition.setId(tablePartIds.remove(0));
                byte[] startKey = MAPPER.realKey(partition.getRange().getStartKey(), partition.getId(), (byte) 't');
                byte[] endKey = MAPPER.nextKey(partition.getId(), (byte) 't');
                partition.getRange().setStartKey(startKey);
                partition.getRange().setEndKey(endKey);
            });
        // create table
        infoSchemaService.createReplicaTable(
            schemaId,
            originPriTableId,
            tableDefinitionWithId
        );

        // table region
        io.dingodb.sdk.service.entity.meta.TableDefinition withIdTableDefinition
            = tableDefinitionWithId.getTableDefinition();
        for (Partition partition : withIdTableDefinition.getTablePartition().getPartitions()) {
            CreateRegionRequest request = CreateRegionRequest
                .builder()
                .regionName("T_" + schemaId + "_" + indexName + "_part_" + partition.getId().getEntityId())
                .regionType(RegionType.STORE_REGION)
                .replicaNum(withIdTableDefinition.getReplica())
                .range(partition.getRange())
                .rawEngine(getRawEngine(withIdTableDefinition.getEngine()))
                .storeEngine(withIdTableDefinition.getStoreEngine())
                .schemaId(schemaId)
                .tableId(originPriTableId)
                .indexId(tableEntityId)
                .partId(partition.getId().getEntityId())
                .tenantId(tableDefinitionWithId.getTenantId())
                .build();
            try {
                LogUtils.info(log, "create replicate region, range:{}", partition.getRange());
                coordinatorService.createRegion(tso(), request);
            } catch (Exception e) {
                LogUtils.error(log, "create replicate region error, range:{}", partition.getRange());
                throw e;
            }
        }

    }

    @Override
    public void rollbackCreateTable(
        long schemaId,
        @NonNull TableDefinition tableDefinition,
        @NonNull List<IndexDefinition> indexTableDefinitions) {
        try {
            LogUtils.info(log, "rollback create table:{}", tableDefinition.getName());
            CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
            io.dingodb.meta.InfoSchemaService schemaService = io.dingodb.meta.InfoSchemaService.root();
            Table table = schemaService.getTableDef(schemaId, tableDefinition.getName());
            if (table == null) {
                LogUtils.info(log, "rollback create table:{}, resource is null", tableDefinition.getName());
                return;
            }

            io.dingodb.meta.MetaService metaService = io.dingodb.meta.MetaService.root();
            metaService.getRangeDistribution(table.tableId).values().forEach(rangeDistribution -> {
                try {
                    coordinatorService.dropRegion(
                        tso(), DropRegionRequest.builder().regionId(rangeDistribution.id().seq).build()
                    );
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage(), e);
                }
            });
            infoSchemaService.dropTable(table.getTableId().domain, table.tableId.seq);
            List<IndexTable> indexes = table.getIndexes();
            if (indexes == null || indexes.isEmpty()) {
                return;
            }

            for (IndexTable index : indexes) {
                metaService.getRangeDistribution(index.tableId).values()
                    .forEach(rangeDistribution -> {
                        try {
                            coordinatorService.dropRegion(
                                tso(), DropRegionRequest.builder().regionId(rangeDistribution.id().seq).build()
                            );
                        } catch (Exception e) {
                            LogUtils.error(log, e.getMessage(), e);
                        }
                    });
            }

            List<CommonId> indexIds = indexes.stream().map(Table::getTableId).collect(Collectors.toList());
            indexIds.forEach(indexId -> infoSchemaService.dropIndex(indexId.domain, indexId.seq));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void createIndex(CommonId tableId, String tableName, IndexDefinition index) {
        validatePartBy(index);
        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
        long indexEntityId = coordinatorService.createIds(
            tso(),
            CreateIdsRequest.builder()
                .idEpochType(IdEpochType.ID_NEXT_TABLE).count(1)
                .build()
        ).getIds().get(0);
        DingoCommonId indexId = DingoCommonId.builder()
            .entityType(EntityType.ENTITY_TYPE_INDEX)
            .parentEntityId(tableId.seq)
            .entityId(indexEntityId)
            .build();
        int count = 0;
        for (PartitionDetailDefinition detail : index.getPartDefinition().getDetails()) {
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
                .parentEntityId(indexId.getEntityId())
                .entityId(id).build())
            .collect(Collectors.toList());
        TableIdWithPartIds indexIdWithPartIds = TableIdWithPartIds.builder()
            .tableId(indexId)
            .partIds(indexPartIds)
            .build();

        TableDefinitionWithId indexWithId = Stream.of(index)
            .map(i -> {
                TableDefinitionWithId td = MAPPER.tableTo(indexIdWithPartIds, i, TenantConstant.TENANT_ID);
                try {
                    MAPPER.resetIndexParameter(td.getTableDefinition(), i);
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage(), e);
                    throw new RuntimeException("invalid index param");
                }
                return td;
            })
            .peek(td -> td.getTableDefinition().setName(tableName + "." + td.getTableDefinition().getName()))
            .findAny().get();
        io.dingodb.meta.InfoSchemaService.root().createIndex(tableId.domain, tableId.seq, indexWithId);
        createIndexRegion(indexWithId, tableId, index.getReplica());
    }

    @Override
    public void dropIndex(CommonId table, CommonId index, long jobId, long startTs) {
        dropRegionByTable(index, jobId, startTs);
        infoSchemaService.dropIndex(table.seq, index.seq);
    }

    @Override
    public Map<CommonId, TableDefinition> getTableIndexDefinitions(@NonNull CommonId id) {
        return infoSchemaService.listIndex(id.domain, id.seq)
            .stream()
            .map(obj -> (TableDefinitionWithId) obj)
            .collect(Collectors.toMap(entry -> MAPPER.idFrom(entry.getTableId()), entry -> {
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
            .columns(table.getColumns().stream().map(MetaService::mapping1).collect(Collectors.toList()))
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
            .schemaState(SchemaState.get(table.getSchemaState().number))
            .properties(prop)
            .build();
    }

    private static ColumnDefinition mapping1(io.dingodb.sdk.service.entity.meta.ColumnDefinition column) {
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
    public long truncateTable(@NonNull String tableName, long tableEntityId, long jobId) {
        return truncateTable(id.getEntityId(), tableName, tableEntityId, jobId);
    }

    @Override
    public long truncateTable(long schemaId, @NonNull String tableName, long tableEntityId, long jobId) {
        // Get old table and indexes
        TableDefinitionWithId table = Optional.mapOrGet(
            infoSchemaService.getTable(schemaId, tableName), __ -> (TableDefinitionWithId) __, () -> null);

        List<Object> indexList = infoSchemaService.listIndex(schemaId, table.getTableId().getEntityId());
        List<TableDefinitionWithId> indexes = indexList.stream()
            .map(object -> (TableDefinitionWithId) object).collect(Collectors.toList());

        // Generate new table ids.
        boolean autoInc = table.getTableDefinition().getColumns().stream()
            .anyMatch(io.dingodb.sdk.service.entity.meta.ColumnDefinition::isAutoIncrement);

        long ts = TsoService.getDefault().tso();
        dropRegionByTable(MAPPER.idFrom(table.getTableId()), jobId, ts, autoInc);

        for (TableDefinitionWithId index : indexes) {
            dropRegionByTable(MAPPER.idFrom(index.getTableId()), jobId, ts);
        }

        List<DingoCommonId> oldIds = new ArrayList<>();
        oldIds.add(table.getTableId());
        indexes.stream().map(TableDefinitionWithId::getTableId)
            .forEach(dingoCommonId ->
                infoSchemaService.dropIndex(dingoCommonId.getParentEntityId(), dingoCommonId.getEntityId()));

        // Reset table id.
        io.dingodb.sdk.service.entity.meta.TableDefinition tableDefinition = table.getTableDefinition();
        DingoCommonId tableId = DingoCommonId.builder()
            .entityType(EntityType.ENTITY_TYPE_TABLE)
            .parentEntityId(schemaId)
            .entityId(tableEntityId)
            .build();
        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
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
        TableIdWithPartIds newTableId =
            TableIdWithPartIds.builder().tableId(tableId).partIds(tablePartIds).build();
        oldIds.forEach(id -> infoSchemaService.dropTable(id.getParentEntityId(), id.getEntityId()));

        resetTableId(newTableId, table);

        // create table、table region
        infoSchemaService.createTableOrView(schemaId, table.getTableId().getEntityId(), table);
        for (Partition partition : tableDefinition.getTablePartition().getPartitions()) {
            CreateRegionRequest request = CreateRegionRequest.builder()
                .regionName("T_" + schemaId + "_" + tableDefinition.getName() + "_part_"
                    + partition.getId().getEntityId())
                .regionType(RegionType.STORE_REGION)
                .replicaNum(tableDefinition.getReplica())
                .range(partition.getRange())
                .rawEngine(RawEngine.RAW_ENG_ROCKSDB)
                .storeEngine(tableDefinition.getStoreEngine())
                .schemaId(schemaId)
                .tableId(table.getTableId().getEntityId())
                .partId(partition.getId().getEntityId())
                .tenantId(table.getTenantId())
                .build();
            try {
                LogUtils.info(log, "create region, range:{}", partition.getRange());
                coordinatorService.createRegion(tso(), request);
            } catch (Exception e) {
                LogUtils.error(log, "create region error,regionId:" + partition.getRange(), e);
                throw e;
            }
        }
        long incrementColCount = tableDefinition.getColumns()
            .stream()
            .filter(io.dingodb.sdk.service.entity.meta.ColumnDefinition::isAutoIncrement)
            .count();
        if (incrementColCount > 0) {
            io.dingodb.sdk.service.MetaService autoIncMetaService
                = Services.autoIncrementMetaService(Configuration.coordinatorSet());
            autoIncMetaService.createAutoIncrement(
                tso(), CreateAutoIncrementRequest.builder()
                    .tableId(tableId)
                    .startId(tableDefinition.getAutoIncrement())
                    .build()
            );
        }
        if (indexes.isEmpty()) {
            return tableEntityId;
        }

        // create index id
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
                        .count(indexDefinitionWithId.getTableDefinition()
                            .getTablePartition().getPartitions().size())
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

            resetTableId(indexIdWithPartIds, indexDefinitionWithId);
            infoSchemaService.createIndex(
                schemaId,
                tableEntityId,
                indexDefinitionWithId
            );
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
                    .regionName("I_" + schemaId + "_" + definition.getName() + "_part_"
                        + partition.getId().getEntityId())
                    .regionType(definition.getIndexParameter().getIndexType() == IndexType.INDEX_TYPE_SCALAR
                        ? RegionType.STORE_REGION : RegionType.INDEX_REGION)
                    .replicaNum(tableDefinition.getReplica())
                    .range(partition.getRange())
                    .rawEngine(RawEngine.RAW_ENG_ROCKSDB)
                    .storeEngine(definition.getStoreEngine())
                    .schemaId(schemaId)
                    .tableId(tableId.getEntityId())
                    .partId(partition.getId().getEntityId())
                    .tenantId(withId.getTenantId())
                    .indexId(withId.getTableId().getEntityId())
                    .indexParameter(indexParameter)
                    .build();
                try {
                    LogUtils.info(log, "create region, range:{}", partition.getRange());
                    coordinatorService.createRegion(tso(), request);
                } catch (Exception e) {
                    LogUtils.error(log, "create region error,schemaId:{},regionId:{}",
                        schemaId, partition.getRange(), e);
                    throw e;
                }
            }
        }
        return tableEntityId;
    }

    public void dropRegionByTable(
        CommonId tableId,
        long jobId,
        long ts,
        boolean autoInc
    ) {
        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
        Collection<RangeDistribution> rangeDistributions = getRangeDistribution(tableId)
            .values();
        LogUtils.info(log, "dropRegion size:{}, tableId:{}", rangeDistributions.size(), tableId);
        deleteRegion(tableId, jobId, ts, autoInc, rangeDistributions);
        invalidateDistribution(tableId);
    }

    public void deleteRegion(
        CommonId tableId,
        long jobId, long startTs, boolean autoInc,
        Collection<RangeDistribution> rangeDistributions
    ) {
        if (ScopeVariables.getNeedGc() && jobId >= 0) {
            Timer.Context context = DingoMetrics.getTimeContext("insertGcDeleteRange");
            gcDeleteRegion(rangeDistributions, jobId, startTs, tableId, autoInc);
            context.stop();
        } else {
            if (autoInc) {
                delAutoInc(tableId);
            }
            CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
            for (RangeDistribution rangeDistribution : rangeDistributions) {
                LogUtils.info(log, "dropRegion id:{}, tableId:{}", rangeDistribution.getId(), tableId);
                try {
                    DropRegionRequest r = DropRegionRequest.builder().regionId(rangeDistribution.id().seq).build();
                    coordinatorService.dropRegion(tso(), r);
                } catch (Exception e) {
                    LogUtils.error(log, "dropRegion id:{} not exists", rangeDistribution.getId().seq);
                }
            }
        }
    }

    private static void resetTableId(TableIdWithPartIds newTableId, TableDefinitionWithId table) {
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
    public boolean dropTable(long schemaId, String tableName, long jobId) {
        return dropTable(TenantConstant.TENANT_ID, schemaId, tableName, jobId);
    }

    @Override
    public boolean dropTable(long tenantId, long schemaId, String tableName, long jobId) {
        io.dingodb.meta.InfoSchemaService schemaService = io.dingodb.meta.InfoSchemaService.root();
        Table table = schemaService.getTableDef(schemaId, tableName, tenantId);
        if (table == null) {
            return false;
        }
        boolean autoInc = table.getColumns().stream().anyMatch(Column::isAutoIncrement);

        long ts = TsoService.getDefault().tso();
        dropRegionByTable(table.getTableId(), jobId, ts, autoInc);

        List<IndexTable> indexes = null;
        if (!"view".equalsIgnoreCase(table.getTableType())) {
            indexes = table.getIndexes();
            if (indexes != null) {
                for (IndexTable index : indexes) {
                    dropRegionByTable(index.getTableId(), jobId, ts);
                }
            }
        }

        infoSchemaService.dropTable(table.getTableId().domain, table.tableId.seq);
        if (!"view".equalsIgnoreCase(table.getTableType())) {
            if (indexes == null) {
                return true;
            }
            List<CommonId> indexIds = indexes.stream().map(Table::getTableId).collect(Collectors.toList());
            indexIds.forEach(indexId -> {
                infoSchemaService.dropIndex(indexId.domain, indexId.seq);
            });
        }
        return true;
    }

    @Override
    public Table getTable(String tableName) {
        return DdlService.root().getTable(name, tableName);
    }

    @Override
    public Table getTable(CommonId tableId) {
        return DdlService.root().getTable(tableId);
    }

    @Override
    public Set<Table> getTables() {
        InfoSchema is = DdlService.root().getIsLatest();
        if (is != null && is.getSchemaMap().containsKey(name)) {
            Collection<Table> tables = is.getSchemaMap()
                .get(name)
                .getTables()
                .values();
            return new HashSet<>(tables);
        }
        return null;
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
        List<Region> regions = coordinatorService.getRegionMap(
                tso, GetRegionMapRequest.builder().tenantId(TenantConstant.TENANT_ID).build()).getRegionmap()
            .getRegions().stream()
            .map(Region::getId)
            .map($ -> coordinatorService.queryRegion(tso, QueryRegionRequest.builder().regionId($).build()).getRegion())
            .collect(Collectors.toList());
        GetSchemasResponse getSchemasResponse
            = service.getSchemas(
            tso, GetSchemasRequest.builder().schemaId(io.dingodb.store.proxy.meta.MetaService.ROOT.id).build()
        );
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
    public long addDistribution(String schemaName, String tableName, PartitionDetailDefinition detail) {
        tableName = cleanTableName(tableName);
        Table table = DdlService.root().getTable(schemaName, tableName);
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
            return commonId.domain;
        }
        LogUtils.warn(log, "Add distribution wait timeout, refresh distributions run in the background.");
        Executors.execute("wait-split", () -> Utils.loop(() -> !checkSplitFinish(comparableKey, table),
            TimeUnit.SECONDS.toNanos(1)));
        return commonId.domain;
    }

    private boolean checkSplitFinish(ComparableByteArray comparableKey, Table table) {
        return comparableKey.compareTo(cache.getRangeDistribution(table.tableId).floorKey(comparableKey)) == 0;
    }

    @Override
    public Object addPart(
        String schemaName, String tableName,
        PartitionDetailDefinition detail,
        long partEntityId,
        Object objWithId
    ) {
        Table table = DdlService.root().getTable(schemaName, tableName);
        if (table == null) {
            throw new RuntimeException("Table not found.");
        }
        TupleType keyType = (TupleType) table.onlyKeyType();
        TupleMapping keyMapping = TupleMapping.of(IntStream.range(0, keyType.fieldCount()).toArray());
        RecordEncoder encoder = new RecordEncoder(
            1, CodecService.createSchemasForType(keyType, keyMapping), 0
        );
        byte[] key = encoder.encodeKeyPrefix(detail.getOperand(), detail.getOperand().length);

        TableDefinitionWithId tableWithId = (TableDefinitionWithId) objWithId;
        List<Partition> partList = tableWithId.getTableDefinition().getTablePartition().getPartitions();
        Partition originPart = partList
            .stream().filter(partition -> partition.getId().getEntityId() == partEntityId)
            .findFirst().orElse(null);

        if (originPart == null) {
            throw DingoErrUtil.newStdErr(ErrUnknown);
        }

        DingoCommonId partId = DingoCommonId.builder()
            .entityType(EntityType.ENTITY_TYPE_PART)
            .parentEntityId(table.tableId.seq)
            .entityId(partEntityId).build();
        byte[] realKey = MAPPER.realKey(key, partId, (byte)'t');
        originPart.getRange().setEndKey(realKey);

        Partition newPart = Partition.builder()
            .range(Range.builder()
                .startKey(realKey)
                .endKey(MAPPER.nextKey(partId, (byte)'t')).build())
            .name(detail.getPartName())
            .id(partId)
            .schemaState(io.dingodb.sdk.service.entity.common.SchemaState.SCHEMA_PUBLIC)
            .build();
        partList.add(newPart);
        List<Partition> newPartList = partList.stream()
            .sorted((p1, p2) -> ByteArrayUtils.compare(p1.getRange().getStartKey(), p2.getRange().getStartKey(), 9))
            .collect(Collectors.toList());
        tableWithId.getTableDefinition().getTablePartition().setPartitions(newPartList);
        return tableWithId;
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

    @Override
    public void rebaseAutoInc(CommonId tableId) {
        AutoIncrementService.INSTANCE.resetAutoIncrement(tableId);
    }

    @Override
    public void resetAutoInc() {
        AutoIncrementService.INSTANCE.resetAutoIncrement();
    }

    @Override
    public void invalidateDistribution(CommonId tableId) {
        this.cache.invalidateDistribution(tableId);
    }

    public void createTenant(Tenant tenant) {
        CreateTenantRequest createTenantRequest = CreateTenantRequest.builder()
            .tenant(mapping(tenant))
            .build();
        this.service.createTenant(System.identityHashCode(createTenantRequest), createTenantRequest);
    }

    public void updateTenant(Tenant tenant) {
        UpdateTenantRequest updateTenantRequest = UpdateTenantRequest.builder()
            .tenant(mapping(tenant))
            .build();
        this.service.updateTenant(System.identityHashCode(updateTenantRequest), updateTenantRequest);
    }

    public void deleteTenant(long tenantId) {
        DropTenantRequest dropTenantRequest = DropTenantRequest.builder()
            .tenantId(tenantId)
            .build();
        this.service.dropTenant(System.identityHashCode(dropTenantRequest), dropTenantRequest);
    }

    public static void validatePartBy(TableDefinition tableDefinition) {
        PartitionDefinition partDefinition = tableDefinition.getPartDefinition();
        switch (partDefinition.getFuncName().toUpperCase()) {
            case DingoPartitionServiceProvider.RANGE_FUNC_NAME:
                DefinitionUtils.checkAndConvertRangePartition(tableDefinition);
                partDefinition.getDetails().add(new PartitionDetailDefinition(null, null, new Object[0]));
                break;
            case DingoPartitionServiceProvider.HASH_FUNC_NAME:
                DefinitionUtils.checkAndConvertHashRangePartition(tableDefinition);
                break;
            default:
                throw new IllegalStateException("Unsupported " + partDefinition.getFuncName());
        }
    }

    public void createIndexRegion(TableDefinitionWithId withId, CommonId tableId, int replica) {
        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
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
                .regionName("I_" + tableId.domain + "_" + definition.getName() + "_part_"
                    + partition.getId().getEntityId())
                .regionType(definition.getIndexParameter().getIndexType() == IndexType.INDEX_TYPE_SCALAR
                    ? RegionType.STORE_REGION : RegionType.INDEX_REGION)
                .replicaNum(replica)
                .range(partition.getRange())
                .rawEngine(getRawEngine(withId.getTableDefinition().getEngine()))
                .storeEngine(definition.getStoreEngine())
                .schemaId(tableId.domain)
                .tableId(tableId.seq)
                .partId(partition.getId().getEntityId())
                .tenantId(withId.getTenantId())
                .indexId(withId.getTableId().getEntityId())
                .indexParameter(indexParameter)
                .build();
            LogUtils.info(log, "create index region, range:{}", partition.getRange());
            try {
                coordinatorService.createRegion(tso(), request);
            } catch (Exception e) {
                LogUtils.error(log, "create index region error, range:{}", partition.getRange());
                throw e;
            }
        }
    }

    public static RawEngine getRawEngine(Engine engine) {
        switch (engine) {
            case BTREE:
            case ENG_BDB:
            case TXN_BTREE:
                return RawEngine.RAW_ENG_BDB;
            default:
                return RawEngine.RAW_ENG_ROCKSDB;
        }
    }

    public void delAutoInc(Object tableId) {
        DingoCommonId commonId;
        if (tableId instanceof DingoCommonId) {
            commonId = (DingoCommonId) tableId;
        } else if (tableId instanceof CommonId) {
            commonId = MAPPER.idTo((CommonId) tableId);
        } else {
            return;
        }

        DeleteAutoIncrementRequest req = DeleteAutoIncrementRequest.builder()
            .tableId(commonId)
            .build();
        try {
            io.dingodb.sdk.service.MetaService metaService
                = Services.autoIncrementMetaService(Configuration.coordinatorSet());
            metaService.deleteAutoIncrement(System.identityHashCode(req), req);
            LogUtils.info(log, "delAutoInc success, tableId:{}", tableId);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        }
    }

    public static void gcDeleteRegion(
        Collection<RangeDistribution> rangeDistributions,
        long jobId,
        long ts,
        CommonId id,
        boolean autoInc
    ) {
        String eleType = id.type.name();
        if (autoInc) {
            eleType = id.type + "_auto";
        }
        String eleId = id.domain + "-" + id.seq;
        int i = 0;
        for (RangeDistribution rangeDistribution : rangeDistributions) {
            CommonId partitionId = new CommonId(CommonId.CommonType.PARTITION, id.seq, rangeDistribution.id().domain);
            byte[] startKey = rangeDistribution.getStartKey();
            byte[] endKey = rangeDistribution.getEndKey();
            startKey = io.dingodb.codec.CodecService.getDefault()
                .setId(startKey, partitionId);
            endKey = io.dingodb.codec.CodecService.getDefault()
                .setId(endKey, partitionId);
            GcDeleteRegion gcDeleteRegion = GcDeleteRegion
                .builder()
                .jobId(jobId)
                .regionId(rangeDistribution.getId().seq)
                .startTs(ts)
                .startKey(ByteArrayUtils.toHex(startKey))
                .endKey(ByteArrayUtils.toHex(endKey))
                .eleId(eleId)
                .build();
            if (i == 0) {
                gcDeleteRegion.setEleType(eleType);
            } else {
                gcDeleteRegion.setEleType(id.type.name());
            }
            Utils.put(DdlUtil.gcDelRegionQueue, gcDeleteRegion);
            LogUtils.info(log, "gcDelete put queue tableId:{}, regionId:{}, startKey:{}, endKey:{}",
                id, gcDeleteRegion.getRegionId(), gcDeleteRegion.getStartTs(), gcDeleteRegion.getEndKey());
            i ++;
        }
    }

}
