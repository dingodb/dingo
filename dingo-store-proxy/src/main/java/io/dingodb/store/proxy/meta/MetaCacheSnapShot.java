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

import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.SchemaTables;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionInfo;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.EntityType;
import io.dingodb.sdk.service.entity.meta.TableDefinition;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.store.service.MetaStoreKv;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.common.CommonId.CommonType.DDL;
import static io.dingodb.common.CommonId.CommonType.META;
import static io.dingodb.common.util.NameCaseUtils.caseSensitive;
import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;
import static io.dingodb.store.proxy.meta.MetaCache.zeroPart;

@Slf4j
public class MetaCacheSnapShot {
    private final InfoSchemaService infoSchemaService;

    public MetaCacheSnapShot(long pointTs) {
        this.infoSchemaService = new io.dingodb.store.service.InfoSchemaService(pointTs);
        DingoMetrics.counter("metaCacheSnapShotInstanceCount").inc();
    }

    public synchronized void clear() {

    }

    public void close() {
        clear();
    }

    private List<TableDefinitionWithId> getIndexes(TableDefinitionWithId tableWithId, DingoCommonId tableId) {
        try {
            if (tableWithId.getTableId().getEntityType() == EntityType.ENTITY_TYPE_INDEX) {
                return new ArrayList<>();
            }
            List<Object> indexList = infoSchemaService
                .listIndex(tableId.getParentEntityId(), tableId.getEntityId());
            return indexList.stream()
                .map(object -> (TableDefinitionWithId) object)
                .peek(indexWithId -> {
                    String name1 = indexWithId.getTableDefinition().getName();
                    String[] split = name1.split("\\.");
                    if (split.length > 1) {
                        name1 = split[split.length - 1];
                    }
                    indexWithId.getTableDefinition().setName(name1);
                })
                .collect(Collectors.toList());
        } catch (Exception e) {
            if (tableWithId != null) {
                LogUtils.error(log, "getIndexes tableWithId:" + tableWithId);
            } else {
                LogUtils.error(log, "getIndexes tableWithId is null");
            }
            throw e;
        }
    }

    @SneakyThrows
    private NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> loadDistribution(CommonId tableId) {
        try {
            if (tableId.type == META || tableId.type == DDL) {
                byte[] startKey = MetaStoreKv.getInstance().getMetaRegionKey();
                byte[] endKey = MetaStoreKv.getInstance().getMetaRegionEndKey();

                if (tableId.type == DDL) {
                    startKey = MetaStoreKv.getDdlInstance().getMetaRegionKey();
                    endKey = MetaStoreKv.getDdlInstance().getMetaRegionEndKey();
                }
                List<Object> regionList = infoSchemaService
                    .scanRegions(startKey, endKey);
                NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> result = new TreeMap<>();
                regionList
                    .forEach(object -> {
                        ScanRegionInfo scanRegionInfo = (ScanRegionInfo) object;
                        RangeDistribution distribution = RangeDistribution.builder()
                            .id(new CommonId(tableId.type,
                                0, scanRegionInfo.getRegionId()))
                            .startKey(scanRegionInfo.getRange().getStartKey())
                            .endKey(scanRegionInfo.getRange().getEndKey())
                            .build();
                        result.put(new ByteArrayUtils.ComparableByteArray(distribution.getStartKey(), 1), distribution);
                    });
                return result;
            }

            TableDefinitionWithId tableWithId = (TableDefinitionWithId) infoSchemaService.getTable(
                tableId
            );
            if (tableWithId == null) {
                LogUtils.error(log, "getTableByStore is null, tableId:{}", tableId);
                InfoSchema is = DdlService.root().getIsLatest();
                Table table = is.getTable(tableId.seq);
                if (table == null) {
                    LogUtils.error(log, "getTableByIs is null, tableId:{}", tableId);
                } else {
                    LogUtils.error(log, "getTableByIs is not null, tableId:{}", tableId);
                }
                return new TreeMap<>();
            }
            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> result = getRangeDistributions(tableWithId);
            if (result.isEmpty()) {
                int retry = 3;
                while (retry-- > 0) {
                    result = getRangeDistributions(tableWithId);
                    if (!result.isEmpty()) {
                        return result;
                    }
                    Utils.sleep(1000);
                }
                return result;
            } else {
                return result;
            }
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            return new TreeMap<>();
        }
    }

    @NonNull
    private NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> getRangeDistributions(TableDefinitionWithId tableWithId) {
        TableDefinition tableDefinition = tableWithId.getTableDefinition();
        List<ScanRegionWithPartId> rangeDistributionList = new ArrayList<>();
        tableDefinition.getTablePartition().getPartitions()
            .forEach(partition -> {
                int isEmptyPartStart = io.dingodb.common.util.ByteArrayUtils.compare(
                    partition.getRange().getStartKey(), zeroPart, true, 1
                );
                int isEmptyPartEnd = io.dingodb.common.util.ByteArrayUtils.compare(
                    partition.getRange().getStartKey(), zeroPart, true, 1
                );
                if (isEmptyPartStart == 0 || isEmptyPartEnd == 0) {
                    LogUtils.error(log, "get table range, but part range error,  " +
                            "tableId:{}, tableName:{}, part:{}",
                        tableWithId.getTableId(), tableDefinition.getName(), partition);
                    throw new RuntimeException("table part is empty");
                }
                List<Object> regionList = infoSchemaService
                    .scanRegions(partition.getRange().getStartKey(), partition.getRange().getEndKey());
                regionList
                    .forEach(object -> {
                        ScanRegionInfo scanRegionInfo = (ScanRegionInfo) object;
                        if (scanRegionInfo.getRegionId() < 80016 && tableWithId.getTableId().getParentEntityId() > 50001) {
                            LogUtils.error(log, "get table range, but get meta region:{}, regionRange:{} " +
                                    "tableId:{}, tableName:{}, part:{}", scanRegionInfo.getRegionId(),
                                scanRegionInfo.getRange(),
                                tableWithId.getTableId(), tableDefinition.getName(), partition);
                            return;
                        }
                        rangeDistributionList.add(
                            new ScanRegionWithPartId(scanRegionInfo, partition.getId().getEntityId())
                        );
                    });
            });
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> result = new TreeMap<>();
        Table table = MAPPER.tableFrom(tableWithId, getIndexes(tableWithId, tableWithId.getTableId()));
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(
            tableDefinition.getCodecVersion(), tableDefinition.getVersion(),
            table.tupleType(), table.keyMapping());
        boolean isOriginalKey = tableDefinition.getTablePartition().getStrategy().number() == 1;
        rangeDistributionList.forEach(scanRegionWithPartId -> {
            RangeDistribution distribution = mapping(scanRegionWithPartId, codec, isOriginalKey);
            if (distribution.getId().seq < 80016) {
                LogUtils.error(log, "get table range, but mapping meta, distributionId:{}, " +
                        "tableId:{}, tableName:{}, partId:{}, regionRange:{}", distribution.getId(),
                    tableWithId.getTableId(), tableDefinition.getName(), scanRegionWithPartId.getPartId(),
                    scanRegionWithPartId.getScanRegionInfo().getRange());
                return;
            }
            result.put(new ByteArrayUtils.ComparableByteArray(distribution.getStartKey(), 1), distribution);
        });
        return result;
    }


    private static RangeDistribution mapping(
        ScanRegionWithPartId scanRegionWithPartId,
        KeyValueCodec codec,
        boolean isOriginalKey
    ) {
        ScanRegionInfo scanRegionInfo = scanRegionWithPartId.getScanRegionInfo();
        byte[] startKey = scanRegionInfo.getRange().getStartKey();
        byte[] endKey = scanRegionInfo.getRange().getEndKey();
        return RangeDistribution.builder()
            .id(new CommonId(
                CommonId.CommonType.DISTRIBUTION, scanRegionWithPartId.getPartId(), scanRegionInfo.getRegionId())
            )
            .startKey(startKey)
            .endKey(endKey)
            .start(codec.decodeKeyPrefix(isOriginalKey ? Arrays.copyOf(startKey, startKey.length) : startKey))
            .end(codec.decodeKeyPrefix(isOriginalKey ? Arrays.copyOf(endKey, endKey.length) : endKey))
            .build();
    }

    public synchronized NavigableMap<String, io.dingodb.store.proxy.meta.MetaService> getMetaServices() {
        InfoSchema infoSchema = DdlService.root().getIsLatest();
        List<SchemaInfo> schemaInfoList;
        if (infoSchema == null) {
            schemaInfoList = infoSchemaService.listSchema();
        } else {
            schemaInfoList
                = infoSchema.getSchemaMap().values().stream()
                .map(SchemaTables::getSchemaInfo).collect(Collectors.toList());
        }
        return getMetaServices(schemaInfoList);
    }

    public NavigableMap<String, io.dingodb.store.proxy.meta.MetaService> getMetaServices(List<SchemaInfo> schemaInfoList) {
        return schemaInfoList
            .stream()
            .filter(schemaInfo -> schemaInfo.getSchemaId() != 0)
            .map(schemaInfo -> {
                DingoCommonId dingoCommonId = DingoCommonId
                    .builder()
                    .entityId(schemaInfo.getSchemaId())
                    .entityType(EntityType.ENTITY_TYPE_SCHEMA)
                    .parentEntityId(0)
                    .build();
                return new io.dingodb.store.proxy.meta.MetaService(dingoCommonId,
                    schemaInfo.getName(), this);
            })
            .collect(Collectors.toMap(
                io.dingodb.store.proxy.meta.MetaService::name,
                Function.identity(),
                (existing, replacement) -> existing,
                () -> caseSensitive() ? new TreeMap<>() : new TreeMap<>(String.CASE_INSENSITIVE_ORDER)));
    }

    @SneakyThrows
    public NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> getRangeDistribution(CommonId id) {
        if (id == null) {
            return new TreeMap<>();
        }
        return loadDistribution(id);
    }
}
