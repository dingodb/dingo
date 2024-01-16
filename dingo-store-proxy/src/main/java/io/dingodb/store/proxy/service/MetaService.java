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

package io.dingodb.store.proxy.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.meta.Meta;
import io.dingodb.meta.MetaServiceProvider;
import io.dingodb.meta.TableStatistic;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.store.proxy.common.PartitionDetailDefinition;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;

import static io.dingodb.common.CommonId.CommonType.SCHEMA;
import static io.dingodb.store.proxy.common.Mapping.mapping;

public class MetaService implements io.dingodb.meta.MetaService {

    public static final MetaService ROOT = new MetaService(
        new MetaServiceClient(Configuration.coordinators()),
        io.dingodb.store.proxy.meta.MetaService.ROOT
    );

    @AutoService(MetaServiceProvider.class)
    public static class Provider implements MetaServiceProvider {
        @Override
        public io.dingodb.meta.MetaService root() {
            return ROOT;
        }
    }

    private MetaService(MetaServiceClient metaServiceClient, io.dingodb.store.proxy.meta.MetaService metaService) {
        this.metaServiceClient = metaServiceClient;
        this.metaService = metaService;
    }

    //
    // Meta service.
    //
    private static Map<CommonId, Long> tableCommitCountMetrics;

    public static CommonId getParentSchemaId(CommonId tableId) {
        return new CommonId(SCHEMA, 0, tableId.domain);
    }

    protected final MetaServiceClient metaServiceClient;
    public final io.dingodb.store.proxy.meta.MetaService metaService;

    @Override
    public io.dingodb.meta.entity.Table getTable(String tableName) {
        return metaService.getTable(tableName);
    }

    @Override
    public io.dingodb.meta.entity.Table getTable(CommonId tableId) {
        return metaService.getTable(tableId);
    }

    @Override
    public boolean truncateTable(@NonNull String tableName) {
        return metaService.truncateTable(tableName);
    }

    @Override
    public Set<io.dingodb.meta.entity.Table> getTables() {
        return metaService.getTables();
    }

    @Override
    public CommonId id() {
        // todo refactor
        Meta.DingoCommonId id = metaServiceClient.id();
        return new CommonId(
            CommonId.CommonType.of(id.getEntityType().getNumber()), id.getParentEntityId(), id.getEntityId()
        );
    }

    @Override
    public String name() {
        return metaServiceClient.name();
    }

    @Override
    public void createSubMetaService(String name) {
        metaServiceClient.createSubMetaService(name);
    }

    @Override
    public Map<String, io.dingodb.meta.MetaService> getSubMetaServices() {
        return metaServiceClient.getSubMetaServices().values().stream()
            .collect(Collectors.toMap(
                MetaServiceClient::name,
                $ -> new MetaService($, metaService.getSubMetaService($.name()))
            ));
    }

    @Override
    public MetaService getSubMetaService(String name) {
        return Optional.mapOrNull(
            metaServiceClient.getSubMetaService(name),
            $ -> new MetaService($, metaService.getSubMetaService($.name()))
        );
    }

    public MetaService getSubMetaService(CommonId id) {
        return Optional.mapOrNull(
            metaServiceClient.getSubMetaService(mapping(id)),
            $ -> new MetaService($, metaService.getSubMetaService($.name()))
        );
    }

    @Override
    public boolean dropSubMetaService(String name) {
        return metaServiceClient.dropSubMetaService(mapping(getSubMetaService(name).id()));
    }

    @Override
    public void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition) {
        metaServiceClient.createTable(tableName, mapping(tableDefinition));
    }

    @Override
    public void createTables(@NonNull TableDefinition tableDefinition,
                             @NonNull List<TableDefinition> indexTableDefinitions) {
        metaService.createTables(tableDefinition, indexTableDefinitions);
    }

    @Override
    public boolean dropTable(@NonNull String tableName) {
        return metaService.dropTable(tableName);
    }

    public boolean dropTables(@NonNull Collection<CommonId> tableIds) {
        return metaService.dropTables(tableIds);
    }

    public CommonId getTableId(@NonNull String tableName) {
        return metaService.getTable(tableName).getTableId();
    }

    @Override
    public synchronized Map<CommonId, Long> getTableCommitCount() {
        return metaService.getTableCommitCount();
    }

    public void addDistribution(String tableName, PartitionDetailDefinition partitionDetail) {
        metaServiceClient.addDistribution(tableName, partitionDetail);
    }

    public RangeDistribution getRangeDistribution(CommonId tableId, CommonId distributionId) {
        return getRangeDistribution(tableId).values().stream()
            .filter(d -> d.id().equals(distributionId))
            .findAny().get();
    }

    @Override
    public NavigableMap<ComparableByteArray, RangeDistribution> getRangeDistribution(CommonId id) {
        return metaService.getRangeDistribution(id);
    }

    //    @Override
//    public NavigableMap<ComparableByteArray, RangeDistribution> getRangeDistribution(CommonId id) {
//        NavigableMap<ComparableByteArray, RangeDistribution> result = new TreeMap<>();
//        io.dingodb.meta.entity.Table table = getTable(id);
//        String funcName = table.getPartitionStrategy();
//        // hash partition strategy need use the original key
//        boolean isOriginalKey = funcName.equalsIgnoreCase("HASH");
//        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(table.tupleType(), table.keyMapping());
//        metaServiceClient.getRangeDistribution(mapping(id)).values().stream()
//            .map(__ -> mapping(__, codec, isOriginalKey))
//            .forEach(__ -> result.put(new ComparableByteArray(__.getStartKey(), 1), __));
//        return result;
//    }
//
//    @Override
//    public NavigableMap<ComparableByteArray, RangeDistribution> getRangeDistribution(CommonId id, io.dingodb.meta.entity.Table tableDefinition) {
//        NavigableMap<ComparableByteArray, RangeDistribution> result = new TreeMap<>();
//        String funcName = tableDefinition.getPartitionStrategy();
//        // hash partition strategy need use the original key
//        boolean isOriginalKey = funcName.equalsIgnoreCase("HASH");
//        KeyValueCodec codec = CodecService.getDefault()
//            .createKeyValueCodec(DingoTypeFactory.INSTANCE.tuple("LONG"), TupleMapping.of(new int[0]));
//        metaServiceClient.getIndexRangeDistribution(mapping(id)).values().stream()
//            .map(__ -> mapping(__, codec, isOriginalKey))
//            .forEach(__ -> result.put(new ComparableByteArray(__.getStartKey()), __));
//        return result;
//    }
//
//    @Override
//    public NavigableMap<ComparableByteArray, RangeDistribution> getRangeDistribution(@NonNull CommonId id) {
//        NavigableMap<ComparableByteArray, RangeDistribution> result = new TreeMap<>();
//        KeyValueCodec codec = CodecService.getDefault()
//            .createKeyValueCodec(DingoTypeFactory.INSTANCE.tuple("LONG"), TupleMapping.of(new int[0]));
//        metaServiceClient.getIndexRangeDistribution(mapping(id)).values().stream()
//            .map(__ -> mapping(__, codec, true))
//            .forEach(__ -> result.put(new ComparableByteArray(__.getStartKey()), __));
//        return result;
//    }

    @Override
    public TableStatistic getTableStatistic(@NonNull String tableName) {
        return metaService.getTableStatistic(tableName);
    }

    @Override
    public TableStatistic getTableStatistic(@NonNull CommonId tableId) {
        return metaService.getTableStatistic(tableId);
    }

    @Override
    public Long getAutoIncrement(CommonId tableId) {
        return metaService.getAutoIncrement(tableId);
    }

    @Override
    public Long getNextAutoIncrement(CommonId tableId) {
        return metaService.getNextAutoIncrement(tableId);
    }

    @Override
    public void updateAutoIncrement(CommonId tableId, long autoIncrementId) {
        metaService.updateAutoIncrement(tableId, autoIncrementId);
    }

}
