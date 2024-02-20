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
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.MetaServiceProvider;
import io.dingodb.meta.TableStatistic;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.PartitionService;
import io.dingodb.sdk.common.serial.RecordEncoder;
import io.dingodb.sdk.service.CoordinatorService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.Region;
import io.dingodb.sdk.service.entity.common.RegionDefinition;
import io.dingodb.sdk.service.entity.coordinator.GetRegionMapRequest;
import io.dingodb.sdk.service.entity.coordinator.QueryRegionRequest;
import io.dingodb.sdk.service.entity.coordinator.RegionCmd.RequestNest.SplitRequest;
import io.dingodb.sdk.service.entity.coordinator.SplitRegionRequest;
import io.dingodb.sdk.service.entity.meta.CreateSchemaRequest;
import io.dingodb.sdk.service.entity.meta.CreateTablesRequest;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.DropSchemaRequest;
import io.dingodb.sdk.service.entity.meta.DropTablesRequest;
import io.dingodb.sdk.service.entity.meta.EntityType;
import io.dingodb.sdk.service.entity.meta.GenerateTableIdsRequest;
import io.dingodb.sdk.service.entity.meta.GetSchemasRequest;
import io.dingodb.sdk.service.entity.meta.GetSchemasResponse;
import io.dingodb.sdk.service.entity.meta.GetTableByNameRequest;
import io.dingodb.sdk.service.entity.meta.GetTableMetricsRequest;
import io.dingodb.sdk.service.entity.meta.GetTableMetricsResponse;
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

    private static Pattern pattern = Pattern.compile("^[A-Z_][A-Z\\d_]*$");
    private static Pattern warnPattern = Pattern.compile(".*[a-z]+.*");

    public final DingoCommonId id;
    public final String name;
    public final io.dingodb.sdk.service.MetaService service;
    public final TsoService tsoService = TsoService.getDefault();
    public final MetaCache cache;
    public final MetaServiceApiImpl api = MetaServiceApiImpl.INSTANCE;

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
    public synchronized void createSubMetaService(String name) {
        if (!MetaServiceApiImpl.INSTANCE.isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
        if (id != ROOT_SCHEMA_ID) {
            throw new UnsupportedOperationException();
        }
        name = cleanSchemaName(name);
        api.createSchema(tso(), name, CreateSchemaRequest.builder().parentSchemaId(id).schemaName(name).build());
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
        if (getSubMetaService(name) == null) {
            return false;
        }
        api.dropSchema(tso(), name, DropSchemaRequest.builder().schemaId(getSubMetaService(name).id).build());
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
        String tableName = cleanTableName(tableDefinition.getName());
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
        List<TableDefinitionWithId> tables = Stream.concat(
                Stream.of(MAPPER.tableTo(tableId, tableDefinition)),
                indexTableDefinitions.stream()
                    .map(indexTableDefinition -> MAPPER.tableTo(tableIds.remove(0), indexTableDefinition))
                    .peek(td -> MAPPER.resetIndexParameter(td.getTableDefinition()))
                    .peek(td -> td.getTableDefinition().setName(tableName + "." + td.getTableDefinition().getName()))
            )
            .collect(Collectors.toList());
        api.createTables(
            tso(), name, tableName, CreateTablesRequest.builder().schemaId(id).tableDefinitionWithIds(tables).build()
        );
        cache.refreshSchema(name);
    }

    @Override
    public boolean truncateTable(@NonNull String tableName) {
        if (!MetaServiceApiImpl.INSTANCE.isReady()) {
            throw new RuntimeException("Offline, please wait and retry.");
        }
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
        api.dropTables(tso(), name, tableName, DropTablesRequest.builder().tableIds(oldIds).build());
        cache.refreshSchema(name);
        service.createTables(
            tso(),
            CreateTablesRequest.builder().schemaId(id).tableDefinitionWithIds(tables).build()
        );
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
        api.dropTables(tso(), name, tableName, DropTablesRequest.builder().tableIds(MAPPER.idTo(tableIds)).build());
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
        log.warn("Add distribution wait timeout, refresh distributions run in the background.");
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

}
