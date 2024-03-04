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

package io.dingodb.web.service;

import io.dingodb.common.Common;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.index.Index;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.cluster.Coordinator;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.index.IndexMetrics;
import io.dingodb.sdk.common.serial.BufImpl;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.table.TableDefinition;
import io.dingodb.sdk.service.cluster.ClusterServiceClient;
import io.dingodb.sdk.service.connector.IndexServiceConnector;
import io.dingodb.sdk.service.connector.StoreServiceConnector;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import io.dingodb.store.Store;
import io.dingodb.web.constant.DingoCluster;
import io.dingodb.web.model.vo.ClusterInfo;
import io.dingodb.web.model.vo.Column;
import io.dingodb.web.model.vo.CoordinatorInfo;
import io.dingodb.web.model.vo.DiskUsageInfo;
import io.dingodb.web.model.vo.ExecutorInfo;
import io.dingodb.web.model.vo.IndexInfo;
import io.dingodb.web.model.vo.Partition;
import io.dingodb.web.model.vo.ProcessInfo;
import io.dingodb.web.model.vo.Region;
import io.dingodb.web.model.vo.RegionDetailInfo;
import io.dingodb.web.model.vo.ResourceExecutorInfo;
import io.dingodb.web.model.vo.ResourceInfo;
import io.dingodb.web.model.vo.SchemaInfo;
import io.dingodb.web.model.vo.StoreDetailInfo;
import io.dingodb.web.model.vo.StoreInfo;
import io.dingodb.web.model.vo.StoreRegions;
import io.dingodb.web.model.vo.TableInfo;
import io.dingodb.web.model.vo.TreeIndex;
import io.dingodb.web.model.vo.TreeSchema;
import io.dingodb.web.model.vo.TreeTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.dingodb.common.util.Utils.buildKeyStr;

@Slf4j
@Service
public class MonitorServerService {

    @Autowired
    PromMetricService promMetricService;

    @Value("${server.monitor.instance.exportPort}")
    private int nodeExportPort;

    @Value("${server.monitor.instance.cpuAlarmThreshold}")
    private int cpuAlarmThreshold;

    @Value("${server.monitor.instance.memAlarmThreshold}")
    private int memAlarmThreshold;

    @Value("${server.monitor.instance.diskAlarmThreshold}")
    private int diskAlarmThreshold;

    @Value("${server.monitor.executor.heapAlarmThreshold}")
    private int heapAlarmThreshold;

    @Autowired
    ClusterServiceClient clusterServiceClient;

    @Autowired
    MetaServiceClient rootMetaServiceClient;

    private static final String timeFormat = "yyyy-MM-dd HH:mm:ss";
    private static final String SCHEMA_TYPE = "schema";
    private static final String TABLE_TYPE = "table";
    private static final String INDEX_TYPE = "index";

    @Cacheable(value = {"navigation"}, key = "#key")
    public List<TreeSchema> getNavigation(String key) {
        return rootMetaServiceClient.getSubMetaServices().entrySet().stream().map(e -> {
            String schemaName = e.getKey().toUpperCase();
            Map<String, Table> tableMap;
            try {
                tableMap = e.getValue().getTableDefinitionsBySchema();
            } catch (Exception ex) {
                return null;
            }
            List<TreeTable> tables = tableMap.keySet().stream()
                .map(table -> {
                    long tableId = e.getValue().getTableId(table).entityId();
                    List<TreeIndex> indices = e.getValue().getTableIndexes(table).entrySet()
                        .stream().map(i ->
                            new TreeIndex(
                                i.getValue().getName(),
                                i.getKey().entityId(),
                                i.getValue().getProperties().get("indexType"),
                                schemaName,
                                table,
                                INDEX_TYPE
                            )
                        )
                        .collect(Collectors.toList());
                    return new TreeTable(table, tableId, indices, schemaName, TABLE_TYPE);
                })
                .collect(Collectors.toList());
            return new TreeSchema(schemaName, tables, SCHEMA_TYPE);
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Cacheable(value = {"tableInfo"}, key = "#key")
    public TableInfo getTableInfo(String schema, String table, String key) {
        // get table definition
        // include column definition and partition definition
        MetaServiceClient metaServiceClient = rootMetaServiceClient.getSubMetaService(schema);
        TableDefinition tableDef = (TableDefinition) metaServiceClient.getTableDefinition(table);
        List<Column> columns = tableDef.getColumns().stream().map(
                e -> new Column(e.getName(), e.getType(), e.isPrimary()))
            .collect(Collectors.toList());
        int partCount = tableDef.getPartition().getDetails().size() + 1;
        Collection<RangeDistribution> rangeDistributions = metaServiceClient.getRangeDistribution(table).values();
        // get table regions count
        int regionsCount = rangeDistributions.size();
        List<Partition> partitions = getPartitionDtoList(tableDef, partCount, rangeDistributions);
        return new TableInfo(partCount, regionsCount, columns, partitions);
    }

    @Cacheable(value = {"partRegion"}, key = "#key")
    public List<Region> getRegionByPart(String schema, String table, Long partId, String key) {
        MetaServiceClient metaServiceClient = rootMetaServiceClient.getSubMetaService(schema);
        TableDefinition tableDefinition = (TableDefinition) metaServiceClient.getTableDefinition(table);
        String funcName = tableDefinition.getPartition().getFuncName();
        boolean isOriginalKey = funcName.equalsIgnoreCase("HASH");
        RangeDistribution[] rangeDistributions = metaServiceClient
            .getRangeDistribution(table).values().toArray(new RangeDistribution[0]);
        DingoKeyValueCodec codec = DingoKeyValueCodec.of(0, tableDefinition);
        DingoType dingoType = DingoTypeFactory.tuple(
            tableDefinition.getColumns().stream().map(col -> DingoTypeFactory.INSTANCE.fromName(
                col.getType(),
                col.getElementType(),
                col.isNullable()
            )).toArray(DingoType[]::new)
        );
        List<Region> regionList = new ArrayList<>();
        for (int i = 0; i < rangeDistributions.length; i++) {
            if (rangeDistributions[i].getId().parentId() == partId) {
                transformRegion(tableDefinition, isOriginalKey, codec, rangeDistributions, regionList, i, dingoType);
            }
        }
        return regionList;
    }

    @Cacheable(value = {"indexPartRegion"}, key = "#key")
    public List<Region> getRegionByIndexPart(String schema, String table, long indexId,
                                             Long partId, String key) {
        MetaServiceClient metaServiceClient = rootMetaServiceClient.getSubMetaService(schema);

        Map<DingoCommonId, Table> indexMap = metaServiceClient.getTableIndexes(table);

        Optional<List<Region>> regionList = indexMap.entrySet()
            .stream()
            .filter(e -> e.getKey().entityId() == indexId)
            .map(e -> {
                String funcName = e.getValue().getPartition().getFuncName();
                boolean isOriginalKey = funcName.equalsIgnoreCase("HASH");
                RangeDistribution[] rangeDistributions
                    = metaServiceClient.getIndexRangeDistribution(e.getKey()).values()
                    .toArray(new RangeDistribution[0]);
                DingoKeyValueCodec codec = DingoKeyValueCodec.of(0, e.getValue());
                DingoType dingoType = DingoTypeFactory.tuple(
                    e.getValue().getColumns().stream().map(col -> DingoTypeFactory.INSTANCE.fromName(
                        col.getType(),
                        col.getElementType(),
                        col.isNullable()
                    )).toArray(DingoType[]::new)
                );
                List<Region> tmpRegionList = new ArrayList<>();
                for (int i = 0; i < rangeDistributions.length; i++) {
                    if (rangeDistributions[i].getId().parentId() == partId) {
                        transformRegion(
                            (TableDefinition) e.getValue(),
                            isOriginalKey,
                            codec,
                            rangeDistributions,
                            tmpRegionList,
                            i,
                            dingoType);
                    }
                }
                return tmpRegionList;
            }).findFirst();
        return regionList.orElseGet(ArrayList::new);
    }

    @Cacheable(value = {"tableRegion"}, key = "#key")
    public List<Region> getRegionByTable(String schema, String table, String key) {
        MetaServiceClient metaServiceClient = rootMetaServiceClient.getSubMetaService(schema);
        TableDefinition tableDefinition = (TableDefinition) metaServiceClient.getTableDefinition(table);
        String funcName = tableDefinition.getPartition().getFuncName();
        boolean isOriginalKey = funcName.equalsIgnoreCase("HASH");
        DingoType dingoType = DingoTypeFactory.tuple(
            tableDefinition.getColumns()
                .stream()
                .map(col -> DingoTypeFactory.INSTANCE.fromName(col.getType(), col.getElementType(), col.isNullable())
                ).toArray(DingoType[]::new)
        );
        RangeDistribution[] rangeDistributions = metaServiceClient
            .getRangeDistribution(table).values().toArray(new RangeDistribution[0]);
        DingoKeyValueCodec codec = DingoKeyValueCodec.of(0, tableDefinition);
        List<Region> regionList = new ArrayList<>();
        for (int i = 0; i < rangeDistributions.length; i++) {
            transformRegion(tableDefinition, isOriginalKey, codec, rangeDistributions, regionList, i, dingoType);
        }
        return regionList;
    }

    @Cacheable(value = {"indexInfo"}, key = "#key")
    public IndexInfo getIndexInfo(String schema,
                                  String table,
                                  long indexId,
                                  String key) {
        MetaServiceClient metaServiceClient = rootMetaServiceClient.getSubMetaService(schema);
        Map<DingoCommonId, Table> indexMap = metaServiceClient.getTableIndexes(table);
        Optional<IndexInfo> tableInfoOptional = indexMap.entrySet().stream()
            .filter(e -> e.getKey().entityId() == indexId).map(e -> {
                int partCount = e.getValue().getPartition().getDetails().size() + 1;
                String indexType = e.getValue().getProperties().get("indexType");
                Long memoryBytes = 0L;
                if (!indexType.equalsIgnoreCase("scalar")) {
                    try {
                        IndexMetrics indexMetrics = metaServiceClient.getIndexMetrics(e.getKey());
                        memoryBytes = indexMetrics.getMemoryBytes();
                    } catch (Exception e1) {
                        log.error(e1.getMessage(), e1);
                    }
                }
                Collection<RangeDistribution> rangeDistributions
                    = metaServiceClient.getIndexRangeDistribution(e.getKey()).values();
                List<Column> columns = e.getValue().getColumns().stream().map(
                        column -> new Column(column.getName(), column.getType(), column.isPrimary()))
                    .collect(Collectors.toList());

                List<Partition> partitionDtoList = getPartitionDtoList((TableDefinition) e.getValue(),
                    partCount, rangeDistributions);
                return new IndexInfo(partCount, rangeDistributions.size(),
                    columns, partitionDtoList, memoryBytes);
            }).findFirst();
        return tableInfoOptional.orElseGet(IndexInfo::new);
    }

    @Cacheable(value = {"indexRegion"}, key = "#key")
    public List<Region> getRegionByIndex(String schema, String table, long indexId, String key) {
        MetaServiceClient metaServiceClient = rootMetaServiceClient.getSubMetaService(schema);
        Map<DingoCommonId, Table> indexMap = metaServiceClient.getTableIndexes(table);
        Optional<List<Region>> regionsOptional = indexMap.entrySet().stream()
            .filter(e -> e.getKey().entityId() == indexId).map(e -> {
                RangeDistribution[] rangeDistributions
                    = metaServiceClient.getIndexRangeDistribution(e.getKey())
                    .values().toArray(new RangeDistribution[0]);
                List<Region> regionDtoList = new ArrayList<>();
                String funcName = e.getValue().getPartition().getFuncName();
                boolean isOriginalKey = funcName.equalsIgnoreCase("HASH");
                DingoKeyValueCodec codec = DingoKeyValueCodec.of(
                    0, e.getValue()
                );
                DingoType dingoType = DingoTypeFactory.tuple(
                    e.getValue().getColumns().stream().map(col -> DingoTypeFactory.INSTANCE.fromName(
                        col.getType(),
                        col.getElementType(),
                        col.isNullable()
                    )).toArray(DingoType[]::new)
                );
                for (int i = 0; i < rangeDistributions.length; i++) {
                    transformRegion((TableDefinition) e.getValue(),
                        isOriginalKey, codec, rangeDistributions, regionDtoList, i, dingoType);
                }
                return regionDtoList;
            }).findFirst();
        return regionsOptional.orElseGet(ArrayList::new);
    }

    //@Cacheable(value = {"cluster"}, key = "#key")
    public ClusterInfo getClusterResource(String key) {
        // get cluster info: cpu, memory, disk
        // get info: schema, table count
        // get coordinator

        // cpu,memory,disk load from prometheus


        // get store
        // get info: cpu, mem, disk, regions count, leader regions count

        // get index
        // get info: cpu, mem, disk, regions count, leader regions count

        // get executor
        // get info: cpu, mem, disk, regions count, leader regions count
        TimeZone timeZone = TimeZone.getTimeZone("GMT+8");
        Calendar calendar = Calendar.getInstance(timeZone);
        long currentTimeSeconds = calendar.getTimeInMillis() / 1000;
        promMetricService.loadMetric(currentTimeSeconds, true);
        Map<String, ResourceInfo> nodeExporterMap = new HashMap<>();
        List<SchemaInfo> schemaList = rootMetaServiceClient
            .getSubMetaServices().entrySet()
            .stream()
            .filter(entry -> !entry.getKey().equalsIgnoreCase("root")
                && !entry.getKey().equalsIgnoreCase("meta"))
            .map(e -> {
                int tableCount = e.getValue().getTableDefinitionsBySchema().size();
                return new SchemaInfo(e.getValue().id().getEntityId(), e.getKey(), tableCount);
            }).collect(Collectors.toList());
        List<Coordinator> coordinatorList = clusterServiceClient.getCoordinatorMap(0);
        List<String> executorList = new ArrayList<>(promMetricService.jvmHeapMap.keySet());
        List<io.dingodb.sdk.common.cluster.Store> storeList = clusterServiceClient.getStoreMap(0);
        // collect store metric
        List<StoreInfo> indexInfoList = storeList
            .stream()
            .filter(store -> store.storeType() == 1 && store.storeState() != 2)
            .map(index -> getStoreInfo(currentTimeSeconds, nodeExporterMap, index))
            .collect(Collectors.toList());
        List<StoreInfo> storeInfoList = storeList
            .stream()
            .filter(store -> store.storeType() == 0 && store.storeState() != 2)
            .map(store -> getStoreInfo(currentTimeSeconds, nodeExporterMap, store))
            .collect(Collectors.toList());

        AtomicInteger coorIndex = new AtomicInteger(0);
        List<CoordinatorInfo> coorInfoList = coordinatorList
            .stream()
            .map(coordinator -> {
                coorIndex.incrementAndGet();
                String instance = coordinator.location().getHost() + ":" + nodeExportPort;
                ResourceInfo resourceInfo = nodeExporterMap.computeIfAbsent(instance,
                    s -> promMetricService.getResource(instance, currentTimeSeconds));
                ProcessInfo processInfo = promMetricService.getProcessMetric(coordinator.location().toString());
                boolean exceedAlarm = exceedAlarm(resourceInfo);
                return new CoordinatorInfo(
                    "coordinator" + coorIndex.get(),
                    coordinator.isLeader(),
                    coordinator.location().getHost(),
                    schemaList,
                    exceedAlarm,
                    processInfo,
                    resourceInfo
                );
            })
            .collect(Collectors.toList());
        AtomicInteger executorIndex = new AtomicInteger(0);
        List<ExecutorInfo> executorInfoList = executorList
            .stream()
            .map(e -> {
                executorIndex.incrementAndGet();
                String host = e.split(":")[0];
                String nodeExporter = host + ":" + nodeExportPort;
                ResourceInfo resourceInfo = nodeExporterMap.computeIfAbsent(nodeExporter,
                    s -> promMetricService.getResource(nodeExporter, currentTimeSeconds));
                ResourceExecutorInfo resource = promMetricService.getResourceExecutorInfo(e, resourceInfo);
                boolean exceedAlarm = exceedExecutorAlarm(resource);
                return new ExecutorInfo(
                    "executor" + executorIndex.get(),
                    host,
                    exceedAlarm,
                    resource
                );
            })
            .collect(Collectors.toList());
        return new ClusterInfo(coorInfoList, executorInfoList, storeInfoList, indexInfoList);
    }

    @NonNull
    private StoreInfo getStoreInfo(long currentTimeSeconds, Map<String, ResourceInfo> nodeExporterMap, io.dingodb.sdk.common.cluster.Store store) {
        String instance = store.serverLocation().getHost() + ":" + nodeExportPort;
        ResourceInfo resourceInfo = nodeExporterMap.computeIfAbsent(instance,
            s -> promMetricService.getResource(instance, currentTimeSeconds));
        boolean exceedAlarm = exceedAlarm(resourceInfo);
        return new StoreInfo(
            store.id(),
            ((Long) store.id()).toString(),
            store.serverLocation().getHost(),
            store.serverLocation().getPort(),
            exceedAlarm,
            new ResourceInfo(0, 0, new ArrayList<>()),
            new ProcessInfo(),
            new StoreRegions(0, 0)
        );
    }

    private boolean exceedExecutorAlarm(ResourceExecutorInfo resourceExecutorInfo) {
        if (exceedAlarm(resourceExecutorInfo)) {
            return true;
        }
        return resourceExecutorInfo.getHeapUsage() > heapAlarmThreshold;
    }

    private boolean exceedAlarm(ResourceInfo resourceInfo) {
        if (resourceInfo.getCpuUsage() * 100 > cpuAlarmThreshold) {
            return true;
        }
        if (resourceInfo.getMemUsage() * 100 > memAlarmThreshold) {
            return true;
        }
        for (DiskUsageInfo diskUsageInfo : resourceInfo.getDiskUsage()) {
            if (diskUsageInfo.getUsage() > diskAlarmThreshold) {
                return true;
            }
        }
        return false;
    }

    public static Index.HelloResponse getIndexRegionsInfo(Location location, boolean getRegionMetrics) {
        IndexServiceConnector indexServiceConnector = new IndexServiceConnector(() -> location);
        Store.Context context = Store.Context.newBuilder()
            .setRegionId(0)
            .setRegionEpoch(Common.RegionEpoch.getDefaultInstance())
            .setIsolationLevelValue(1)
            .build();
        Index.HelloRequest helloRequest = Index.HelloRequest.newBuilder()
            .setContext(context)
            .setGetRegionMetrics(getRegionMetrics)
            .build();
        return indexServiceConnector.exec(stub -> stub.hello(helloRequest));
    }

    public static Store.HelloResponse getStoreRegionsInfo(Location location, boolean getRegionMetrics) {
        StoreServiceConnector storeServiceConnector = new StoreServiceConnector(() -> location);
        Store.Context context = Store.Context.newBuilder()
            .setRegionId(0)
            .setRegionEpoch(Common.RegionEpoch.getDefaultInstance())
            .setIsolationLevelValue(1)
            .build();
        Store.HelloRequest helloRequest = Store.HelloRequest.newBuilder()
            .setContext(context)
            .setGetRegionMetrics(getRegionMetrics)
            .build();
        return storeServiceConnector.exec(stub -> stub.hello(helloRequest));
    }

    private List<Partition> getPartitionDtoList(TableDefinition tableDef,
                                                       int partCount,
                                                       Collection<RangeDistribution> rangeDistributions) {
        DingoKeyValueCodec codec = DingoKeyValueCodec.of(0, tableDef);
        String funcName = tableDef.getPartition().getFuncName();
        boolean isOriginalKey = funcName.equalsIgnoreCase("HASH");
        List<Long> partIdList = new ArrayList<>(0);
        AtomicLong partColSizeAto = new AtomicLong();
        rangeDistributions.forEach(r -> {
            if (!partIdList.contains(r.getId().parentId())) {
                partIdList.add(r.getId().parentId());
            }
            if (partColSizeAto.get() == 0) {
                setId(r.getRange().startKey);
                Object[] start = codec.decodeKeyPrefix(isOriginalKey ? Arrays.copyOf(r.getRange().startKey,
                    r.getRange().startKey.length) : r.getRange().startKey);
                long partByColCount = Arrays.stream(start).filter(Objects::nonNull).count();
                partColSizeAto.set(partByColCount);
            }
        });
        long partColSize = partColSizeAto.get();
        if (partColSize == 0) {
            partColSize = tableDef.getPrimaryKeyCount();
        }
        partIdList.sort(Comparator.reverseOrder());

        String partType = tableDef.getPartition().getFuncName();
        List<Partition> partitions = new ArrayList<>();
        for (int i = 0; i < partCount; i++) {
            String partName = partIdList.get(i).toString();
            List<String> keyNameList = new ArrayList<>();
            for (int c = 0; c < partColSize; c ++) {
                keyNameList.add(tableDef.getKeyColumns().get(c).getName());
            }
            String cols = StringUtils.join(keyNameList);
            partitions.add(new Partition(partIdList.get(i),
                partName, partType, cols));
        }
        return partitions;
    }

    private void transformRegion(TableDefinition tableDefinition,
                                 boolean isOriginalKey,
                                 DingoKeyValueCodec codec,
                                 RangeDistribution[] rangeDistributions,
                                 List<Region> regionDtoList,
                                 int index,
                                 DingoType dingoType) {
        RangeDistribution rangeDistribution = rangeDistributions[index];
        Object[] start;
        Object[] end;
        setId(rangeDistribution.getRange().startKey);
        start = codec.decodeKeyPrefix(isOriginalKey ? Arrays.copyOf(rangeDistribution.getRange().startKey,
            rangeDistribution.getRange().startKey.length) : rangeDistribution.getRange().startKey);
        if (index + 1 < rangeDistributions.length) {
            byte[] nextStart = rangeDistributions[index + 1].getRange().startKey;
            setId(nextStart);
            end = codec.decodeKeyPrefix(isOriginalKey ? Arrays.copyOf(nextStart,
                nextStart.length) : nextStart);
        } else {
            end = null;
        }
        start = (Object[]) dingoType.convertFrom(start, DingoConverter.INSTANCE);
        end = (Object[]) dingoType.convertFrom(end, DingoConverter.INSTANCE);
        String startKey = buildKeyStr(TupleMapping.of(tableDefinition.getKeyColumnIndices()), start);
        String endKey = buildKeyStr(TupleMapping.of(tableDefinition.getKeyColumnIndices()), end);
        String range = String.format("[ %s, %s )", startKey, endKey);
        rangeDistribution.getVoters().remove(rangeDistribution.getLeader());
        regionDtoList.add(new Region(
            rangeDistribution.getId().entityId(),
            rangeDistribution.getLeader().toString(),
            StringUtils.join(rangeDistribution.getVoters()),
            range, 0));
    }

    @Cacheable(value = {"regionDetail"}, key = "#regionId")
    public RegionDetailInfo getRegion(long regionId) {
        io.dingodb.sdk.common.cluster.Region region = clusterServiceClient.queryRegion(regionId);
        String type = region.regionType() == 0 ? "store" : "index";
        String state = DingoCluster.regionMap.getOrDefault(region.regionState(), "REGION_NONE");
        SimpleDateFormat df = new SimpleDateFormat(timeFormat);
        String createTime = df.format(new Date(region.createTime()));
        String deleteTime = null;
        if (region.deleteTime() > 0) {
            deleteTime = df.format(new Date(region.deleteTime()));
        }
        List<String> followers = region.followers().stream().map(Location::toString).collect(Collectors.toList());
        return new RegionDetailInfo(type, state, createTime, deleteTime, followers, region.leader().toString());
    }

    @Cacheable(value = {"processRegion"}, key = "#key")
    public List<Region> getStoreProcessRegions(String host, int port, String key) {
        try {
            Store.HelloResponse response = getStoreRegionsInfo(new Location(host, port), true);
            return response.getRegionMetricsList()
                .stream()
                .map(p -> new Region(p.getId(), "", "", "[ Infinity, Infinity ]", 0))
                .collect(Collectors.toList());
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    @Cacheable(value = {"processInfo"}, key = "#id")
    public StoreDetailInfo getStoreProcessInfo(Long id, Location serverLocation) {
        Store.HelloResponse response = getStoreRegionsInfo(serverLocation, false);
        StoreRegions storeRegions
            = new StoreRegions(response.getRegionCount(), response.getRegionLeaderCount());
        return getStoreDetailInfo(id, serverLocation, storeRegions);
    }


    @Cacheable(value = {"processRegion"}, key = "#key")
    public List<Region> getIndexProcessRegions(String host, int port, String key) {
        try {
            Index.HelloResponse response = getIndexRegionsInfo(new Location(host, port), true);
            return response.getRegionMetricsList()
                .stream()
                .map(p -> new Region(p.getId(),
                    "", "", "[ Infinity, Infinity ]", p.getLeaderStoreId()))
                .collect(Collectors.toList());
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    @Cacheable(value = {"processInfo"}, key = "#id")
    public StoreDetailInfo getIndexProcessInfo(Long id, Location serverLocation) {
        Index.HelloResponse response = getIndexRegionsInfo(serverLocation, false);
        StoreRegions storeRegions
            = new StoreRegions(response.getRegionCount(), response.getRegionLeaderCount());
        return getStoreDetailInfo(id, serverLocation, storeRegions);
    }

    @Cacheable(value = {"regionMap"}, key = "#key")
    public Map<Long, List<Region>> getRegionMap(String key) {
        List<io.dingodb.sdk.common.cluster.Region> regionList = clusterServiceClient.getRegionMap(0);
        return regionList.stream()
            .map(r -> new Region(r.regionId(),
                "", "", "[ Infinity, Infinity ]", r.leaderStoreId()))
            .collect(Collectors.groupingBy(Region::getLeaderStoreId));
    }

    public StoreDetailInfo getStoreDetailInfo(Long id, Location serverLocation, StoreRegions storeRegions) {
        String instance = serverLocation.getHost() + ":" + nodeExportPort;
        TimeZone timeZone = TimeZone.getTimeZone("GMT+8");
        Calendar calendar = Calendar.getInstance(timeZone);
        long currentTimeSeconds = calendar.getTimeInMillis() / 1000;
        promMetricService.loadMetric(currentTimeSeconds, false);
        ResourceInfo resourceInfo = promMetricService.getResource(instance, currentTimeSeconds);
        ProcessInfo processInfo = promMetricService.getProcessMetric(serverLocation.toString());
        boolean exceedAlarm = exceedAlarm(resourceInfo);
        return new StoreDetailInfo(
            id,
            id.toString(),
            serverLocation.getHost(),
            serverLocation.getPort(),
            exceedAlarm,
            resourceInfo,
            processInfo,
            storeRegions);
    }

    private void setId(byte[] key) {
        BufImpl buf = new BufImpl(key);
        buf.skip(1);
        buf.writeLong(0);
    }

}
