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

package io.dingodb.store.service;

import com.google.auto.service.AutoService;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.expr.core.TypeCode;
import io.dingodb.meta.Meta;
import io.dingodb.meta.MetaServiceProvider;
import io.dingodb.meta.TableStatistic;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import io.dingodb.store.Configuration;
import io.dingodb.store.common.Mapping;
import io.dingodb.store.common.PartitionDetailDefinition;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static io.dingodb.common.CommonId.CommonType.SCHEMA;
import static io.dingodb.store.common.Mapping.mapping;

public class MetaService implements io.dingodb.meta.MetaService {

    public static final MetaService ROOT = new MetaService(new MetaServiceClient(Configuration.coordinators()));

    @AutoService(MetaServiceProvider.class)
    public static class Provider implements MetaServiceProvider {
        @Override
        public io.dingodb.meta.MetaService root() {
            return ROOT;
        }
    }

    private MetaService(MetaServiceClient metaServiceClient) {
        this.metaServiceClient = metaServiceClient;
    }

    //
    // Meta service.
    //
    private static Map<CommonId, Long> tableCommitCountMetrics;

    public static CommonId getParentSchemaId(CommonId tableId) {
        return new CommonId(SCHEMA, 0, tableId.domain);
    }

    protected final MetaServiceClient metaServiceClient;

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
            .collect(Collectors.toMap(MetaServiceClient::name, MetaService::new));
    }

    @Override
    public MetaService getSubMetaService(String name) {
        return new MetaService(metaServiceClient.getSubMetaService(name));
    }

    public MetaService getSubMetaService(CommonId id) {
        return new MetaService(metaServiceClient.getSubMetaService(mapping(id)));
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
        List<Table> indexTables = indexTableDefinitions.stream().map(Mapping::mapping).collect(Collectors.toList());
        indexTables.forEach(__ -> {
           io.dingodb.store.common.TableDefinition table =
                (io.dingodb.store.common.TableDefinition) __;
            table.setProperties(__.getProperties());
            table.setName(tableDefinition.getName() + "." + __.getName());
        });
        metaServiceClient.createTables(mapping(tableDefinition), indexTables);
    }

    @Override
    public boolean dropTable(@NonNull String tableName) {
        return metaServiceClient.dropTable(tableName);
    }

    @Override
    public boolean dropTables(@NonNull Collection<CommonId> tableIds) {
        return metaServiceClient.dropTables(
            tableIds.stream().map(Mapping::mapping).collect(Collectors.toCollection(ArrayList::new))
        );
    }

    @Override
    public CommonId getTableId(@NonNull String tableName) {
        return Optional.mapOrNull(metaServiceClient.getTableId(tableName), Mapping::mapping);
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
        if (id().seq == 0) {
            return Collections.emptyMap();
        }
        return metaServiceClient.getTableDefinitionsBySchema().values().stream()
            .collect(Collectors.toMap(Table::getName, Mapping::mapping));
    }

    @Override
    public List<TableDefinition> getTableDefinitions(@NonNull String name) {
        return metaServiceClient.getTables(name).stream().map(Mapping::mapping).collect(Collectors.toList());
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull String name) {
        return Optional.mapOrNull(metaServiceClient.getTableDefinition(name), Mapping::mapping);
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull CommonId id) {
        return Optional.mapOrNull(metaServiceClient.getTableDefinition(mapping(id)), Mapping::mapping);
    }

    @Override
    public synchronized Map<CommonId, Long> getTableCommitCount() {
        if (this == ROOT) {
            return metaServiceClient.getTableCommitCount().entrySet().stream()
                .collect(Collectors.toMap(e -> mapping(e.getKey()), Map.Entry::getValue));
        }
        throw new UnsupportedOperationException("Only supported root schema.");
    }

    @Override
    public synchronized Map<CommonId, Long> getTableCommitIncrement() {
        if (this == ROOT) {
            Map<CommonId, Long> result = new HashMap<>();
            Map<CommonId, Long> newMetrics = metaServiceClient.getTableCommitCount().entrySet().stream()
                .collect(Collectors.toMap(e -> mapping(e.getKey()), Map.Entry::getValue));
            if (tableCommitCountMetrics == null) {
                tableCommitCountMetrics = newMetrics;
            }
            newMetrics.forEach((id, i) -> result.put(id, i - tableCommitCountMetrics.getOrDefault(id, 0L)));
            tableCommitCountMetrics = newMetrics;
            return result;
        }
        throw new UnsupportedOperationException("Only supported root schema.");
    }

    @Override
    public Map<CommonId, TableDefinition> getTableIndexDefinitions(@NonNull CommonId id) {
        return metaServiceClient.getTableIndexes(mapping(id)).entrySet().stream()
           .collect(Collectors.toMap(entry -> mapping(entry.getKey()), entry -> {
               // Remove . from the index table name
               Table table = entry.getValue();
               String tableName = table.getName();
               String[] split = tableName.split("\\.");
               if (split.length > 1) {
                   tableName = split[split.length - 1];
               }
               return mapping(table).copyWithName(tableName);
           }));
    }

    @Override
    public Map<CommonId, TableDefinition> getTableIndexDefinitions(@NonNull String name) {
        return metaServiceClient.getTableIndexes(name).entrySet().stream()
            .collect(Collectors.toMap(entry -> mapping(entry.getKey()), entry -> {
                // Remove . from the index table name
                Table table = entry.getValue();
                String tableName = table.getName();
                String[] split = tableName.split("\\.");
                if (split.length > 1) {
                    tableName = split[split.length - 1];
                }
                return mapping(table).copyWithName(tableName);
            }));
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
        NavigableMap<ComparableByteArray, RangeDistribution> result = new TreeMap<>();
        TableDefinition tableDefinition = getTableDefinition(id);
        String funcName = tableDefinition.getPartDefinition().getFuncName();
        // hash partition strategy need use the original key
        boolean isOriginalKey = funcName.equalsIgnoreCase("HASH");
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(tableDefinition);
        metaServiceClient.getRangeDistribution(mapping(id)).values().stream()
            .map(__ -> mapping(__, codec, isOriginalKey))
            .forEach(__ -> result.put(new ComparableByteArray(__.getStartKey()), __));
        return result;
    }

    @Override
    public NavigableMap<ComparableByteArray, RangeDistribution> getIndexRangeDistribution(
        @NonNull CommonId id,
        TableDefinition tableDefinition
    ) {
        NavigableMap<ComparableByteArray, RangeDistribution> result = new TreeMap<>();
        String funcName = tableDefinition.getPartDefinition().getFuncName();
        // hash partition strategy need use the original key
        boolean isOriginalKey = funcName.equalsIgnoreCase("HASH");
        KeyValueCodec codec = CodecService.getDefault()
            .createKeyValueCodec(DingoTypeFactory.tuple(TypeCode.LONG), TupleMapping.of(new int[0]));
        metaServiceClient.getIndexRangeDistribution(mapping(id)).values().stream()
            .map(__ -> mapping(__, codec, isOriginalKey))
            .forEach(__ -> result.put(new ComparableByteArray(__.getStartKey()), __));
        return result;
    }

    @Override
    public NavigableMap<ComparableByteArray, RangeDistribution> getIndexRangeDistribution(@NonNull CommonId id) {
        NavigableMap<ComparableByteArray, RangeDistribution> result = new TreeMap<>();
        KeyValueCodec codec = CodecService.getDefault()
            .createKeyValueCodec(DingoTypeFactory.tuple(TypeCode.LONG), TupleMapping.of(new int[0]));
        metaServiceClient.getIndexRangeDistribution(mapping(id)).values().stream()
            .map(__ -> mapping(__, codec, true))
            .forEach(__ -> result.put(new ComparableByteArray(__.getStartKey()), __));
        return result;
    }

    @Override
    public TableStatistic getTableStatistic(@NonNull String tableName) {
        return new TableStatistic() {

            @Override
            public byte[] getMinKey() {
                return metaServiceClient.getTableMetrics(tableName).getMinKey();
            }

            @Override
            public byte[] getMaxKey() {
                return metaServiceClient.getTableMetrics(tableName).getMaxKey();
            }

            @Override
            public long getPartCount() {
                return metaServiceClient.getTableMetrics(tableName).getPartCount();
            }

            @Override
            public Double getRowCount() {
                return (double) metaServiceClient.getTableMetrics(tableName).getRowCount();
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

}
