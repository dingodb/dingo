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

import com.codahale.metrics.CachedGauge;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.SchemaTables;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.service.MetaService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.RegionDefinition;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionInfo;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.EntityType;
import io.dingodb.sdk.service.entity.meta.MetaEvent;
import io.dingodb.sdk.service.entity.meta.MetaEventRegion;
import io.dingodb.sdk.service.entity.meta.MetaEventType;
import io.dingodb.sdk.service.entity.meta.TableDefinition;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.sdk.service.entity.meta.WatchRequest;
import io.dingodb.sdk.service.entity.meta.WatchRequest.RequestUnionNest.CreateRequest;
import io.dingodb.sdk.service.entity.meta.WatchRequest.RequestUnionNest.ProgressRequest;
import io.dingodb.sdk.service.entity.meta.WatchResponse;
import io.dingodb.store.proxy.service.TsoService;
import io.dingodb.store.service.MetaStoreKv;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.common.CommonId.CommonType.DDL;
import static io.dingodb.common.CommonId.CommonType.INDEX;
import static io.dingodb.common.CommonId.CommonType.META;
import static io.dingodb.common.CommonId.CommonType.TABLE;
import static io.dingodb.sdk.service.entity.meta.MetaEventType.META_EVENT_REGION_CREATE;
import static io.dingodb.sdk.service.entity.meta.MetaEventType.META_EVENT_REGION_DELETE;
import static io.dingodb.sdk.service.entity.meta.MetaEventType.META_EVENT_REGION_UPDATE;
import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;
import static java.lang.Math.max;

@Slf4j
public class MetaCache {

    private final MetaService metaService;
    private final InfoSchemaService infoSchemaService;
    private final TsoService tsoService;

    private final LoadingCache<CommonId, NavigableMap<ComparableByteArray, RangeDistribution>> distributionCache;

    private boolean isClose = false;

    private static int cnt = 0;

    public MetaCache(Set<Location> coordinators) {
        this.metaService = Services.metaService(coordinators);
        this.infoSchemaService = InfoSchemaService.root();
        this.tsoService = TsoService.INSTANCE.isAvailable() ? TsoService.INSTANCE : new TsoService(coordinators);
        this.distributionCache = buildDistributionCache();
        Executors.execute("watch-meta", () -> {
            while (!isClose) {
                try {
                    watch();
                } catch (Exception e) {
                    LogUtils.error(log, "Watch meta error, restart watch.", e);
                }
            }
        });
        cnt ++;
        DingoMetrics.metricRegistry.register("distributionCache" + cnt, new CachedGauge<Long>(1, TimeUnit.MINUTES) {
            @Override
            protected Long loadValue() {
                return distributionCache.size();
            }
        });
        DingoMetrics.counter("metaCacheInstanceCount").inc();
    }

    public MetaCache(Set<Location> coordinators, long pointTs) {
        this.metaService = Services.metaService(coordinators);
        this.infoSchemaService = new io.dingodb.store.service.InfoSchemaService(pointTs);
        this.tsoService = TsoService.INSTANCE.isAvailable() ? TsoService.INSTANCE : new TsoService(coordinators);
        this.distributionCache = buildDistributionCache();
        Executors.execute("watch-meta", () -> {
            while (!isClose) {
                try {
                    watch();
                } catch (Exception e) {
                    LogUtils.error(log, "Watch meta error, restart watch.", e);
                }
            }
        });
        DingoMetrics.counter("metaCacheInstanceCount").inc();
    }

    private long tso() {
        return tsoService.tso();
    }

    public synchronized void clear() {
        distributionCache.invalidateAll();
    }

    public void close() {
        clear();
        isClose = true;
    }

    private void watch() {
        WatchResponse response = metaService.watch(
            tso(),
            WatchRequest.builder().requestUnion(CreateRequest.builder().eventTypes(eventTypes()).build()).build()
        );
        clear();
        long watchId = response.getWatchId();
        long revision = -1;
        while (!isClose) {
            response = metaService.watch(
                tso(),
                WatchRequest.builder().requestUnion(ProgressRequest.builder().watchId(watchId).build()).build()
            );
            if (revision > 0 && revision < response.getCompactRevision()) {
                LogUtils.info(log,
                    "Watch id {} out, revision {}, compact revision {}, restart watch.",
                    watchId, revision, response.getCompactRevision()
                );
                return;
            }
            if (Parameters.cleanNull(response.getEvents(), Collections.EMPTY_LIST).isEmpty()) {
                continue;
            }
            for (MetaEvent event : response.getEvents()) {
                LogUtils.info(log, "Receive meta event: {}", event);
                switch (event.getEventType()) {
                    case META_EVENT_NONE:
                        break;
                    case META_EVENT_REGION_CREATE:
                    case META_EVENT_REGION_UPDATE:
                    case META_EVENT_REGION_DELETE: {
                        invalidateDistribution((MetaEventRegion) event.getEvent());
                        revision = max(revision, ((MetaEventRegion) event.getEvent()).getDefinition().getRevision());
                        break;
                    }
                    default:
                        throw new IllegalStateException("Unexpected value: " + event.getEventType());
                }
            }
        }
    }

    @NonNull
    private static List<MetaEventType> eventTypes() {
        return Arrays.asList(
            META_EVENT_REGION_CREATE,
            META_EVENT_REGION_UPDATE,
            META_EVENT_REGION_DELETE
        );
    }

    private LoadingCache<CommonId, NavigableMap<ComparableByteArray, RangeDistribution>> buildDistributionCache() {
        return CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES).expireAfterWrite(10, TimeUnit.MINUTES)
            .build(new CacheLoader<CommonId, NavigableMap<ComparableByteArray, RangeDistribution>>() {
                @Override
                public NavigableMap<ComparableByteArray, RangeDistribution> load(CommonId key) {
                    return loadDistribution(key);
                }
            });
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
    private NavigableMap<ComparableByteArray, RangeDistribution> loadDistribution(CommonId tableId) {
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
                NavigableMap<ComparableByteArray, RangeDistribution> result = new TreeMap<>();
                regionList
                    .forEach(object -> {
                        ScanRegionInfo scanRegionInfo = (ScanRegionInfo) object;
                        RangeDistribution distribution = RangeDistribution.builder()
                            .id(new CommonId(tableId.type,
                                0, scanRegionInfo.getRegionId()))
                            .startKey(scanRegionInfo.getRange().getStartKey())
                            .endKey(scanRegionInfo.getRange().getEndKey())
                            .build();
                        result.put(new ComparableByteArray(distribution.getStartKey(), 1), distribution);
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
                return null;
            }
            TableDefinition tableDefinition = tableWithId.getTableDefinition();
            List<ScanRegionWithPartId> rangeDistributionList = new ArrayList<>();
            tableDefinition.getTablePartition().getPartitions()
                .forEach(partition -> {
                    List<Object> regionList = infoSchemaService
                        .scanRegions(partition.getRange().getStartKey(), partition.getRange().getEndKey());
                    regionList
                        .forEach(object -> {
                            ScanRegionInfo scanRegionInfo = (ScanRegionInfo) object;
                            rangeDistributionList.add(
                                new ScanRegionWithPartId(scanRegionInfo, partition.getId().getEntityId())
                            );
                        });
                });
            NavigableMap<ComparableByteArray, RangeDistribution> result = new TreeMap<>();
            Table table = MAPPER.tableFrom(tableWithId, getIndexes(tableWithId, tableWithId.getTableId()));
            KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(
                tableDefinition.getCodecVersion(), tableDefinition.getVersion(),
                table.tupleType(), table.keyMapping());
            boolean isOriginalKey = tableDefinition.getTablePartition().getStrategy().number() == 1;
            rangeDistributionList.forEach(scanRegionWithPartId -> {
                RangeDistribution distribution = mapping(scanRegionWithPartId, codec, isOriginalKey);
                result.put(new ComparableByteArray(distribution.getStartKey(), 1), distribution);
            });
            return result;
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            return null;
        }
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

    public void invalidateDistribution(MetaEventRegion metaEventRegion) {
        RegionDefinition definition = metaEventRegion.getDefinition();
        LogUtils.info(log, "Invalid table distribution {}", definition);
        if (definition.getSchemaId() == 1001) {
            distributionCache.invalidate(new CommonId(META, 0, 0));
        } else if (definition.getSchemaId() == 1002) {
            distributionCache.invalidate(new CommonId(DDL, 0, 0));
        } else {
            distributionCache.invalidate(new CommonId(TABLE, definition.getSchemaId(), definition.getTableId()));
            if (definition.getIndexId() != 0) {
                distributionCache.invalidate(new CommonId(INDEX, definition.getTableId(), definition.getIndexId()));
            }
        }
    }

    public void invalidateDistribution(CommonId tableId) {
        LogUtils.info(log, "Invalid table distribution {}", tableId);
        distributionCache.invalidate(tableId);
    }

    public synchronized Map<String, io.dingodb.store.proxy.meta.MetaService> getMetaServices() {
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

    public Map<String, io.dingodb.store.proxy.meta.MetaService> getMetaServices(List<SchemaInfo> schemaInfoList) {
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
                    schemaInfo.getName().toUpperCase(), metaService, this);
            })
            .collect(Collectors.toMap(io.dingodb.store.proxy.meta.MetaService::name, Function.identity()));
    }

    @SneakyThrows
    public NavigableMap<ComparableByteArray, RangeDistribution> getRangeDistribution(CommonId id) {
        if (id == null) {
            return new TreeMap<>();
        }
        return distributionCache.get(id);
    }

}
