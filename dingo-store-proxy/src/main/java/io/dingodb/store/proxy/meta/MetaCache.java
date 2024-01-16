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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.service.MetaService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.GetIndexRangeRequest;
import io.dingodb.sdk.service.entity.meta.GetSchemaByNameRequest;
import io.dingodb.sdk.service.entity.meta.GetTableByNameRequest;
import io.dingodb.sdk.service.entity.meta.GetTableByNameResponse;
import io.dingodb.sdk.service.entity.meta.GetTableRangeRequest;
import io.dingodb.sdk.service.entity.meta.GetTableRequest;
import io.dingodb.sdk.service.entity.meta.GetTablesRequest;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.store.proxy.service.TsoService;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.dingodb.common.util.ByteArrayUtils.greatThan;
import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;

public class MetaCache {

    @EqualsAndHashCode
    @AllArgsConstructor
    private class Names {
        final String schema;
        final String table;
    }

    private final Set<Location> coordinators;
    private final MetaService metaService;
    private final TsoService tsoService;

    private final LoadingCache<Names, Optional<Table>> tableNameCache;
    private final LoadingCache<CommonId, Table> tableIdCache;

    private final LoadingCache<CommonId, NavigableMap<ComparableByteArray, RangeDistribution>> distributionCache;

    public MetaCache(Set<Location> coordinators) {
        this.coordinators = coordinators;
        this.metaService = Services.metaService(coordinators);
        this.tsoService = TsoService.INSTANCE.isAvailable() ? TsoService.INSTANCE : new TsoService(coordinators);
        this.tableNameCache = buildTableNameCache();
        this.tableIdCache = buildTableIdCache();
        this.distributionCache = buildDistributionCache();
    }

    private long tso() {
        return tsoService.tso();
    }

    private LoadingCache<Names, Optional<Table>> buildTableNameCache() {
        return CacheBuilder.newBuilder()
            .expireAfterAccess(60, TimeUnit.MINUTES).expireAfterWrite(60, TimeUnit.MINUTES)
            .build(new CacheLoader<Names, Optional<Table>>() {
                @Override
                public Optional<Table> load(Names names) throws Exception {
                    return loadTable(names);
                }
            });
    }

    private LoadingCache<CommonId, Table> buildTableIdCache() {
        return CacheBuilder.newBuilder()
            .expireAfterAccess(60, TimeUnit.MINUTES).expireAfterWrite(60, TimeUnit.MINUTES)
            .build(new CacheLoader<CommonId, Table>() {
                @Override
                public Table load(CommonId tableId) throws Exception {
                    return loadTable(tableId);
                }
            });
    }

    private LoadingCache<CommonId, NavigableMap<ComparableByteArray, RangeDistribution>> buildDistributionCache() {
        return CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES).expireAfterWrite(10, TimeUnit.MINUTES).maximumSize(64)
            .build(new CacheLoader<CommonId, NavigableMap<ComparableByteArray, RangeDistribution>>() {
                @Override
                public NavigableMap<ComparableByteArray, RangeDistribution> load(CommonId key) throws Exception {
                    return loadDistribution(key);
                }
            });
    }

    private synchronized Table loadTable(CommonId tableId) {
        TableDefinitionWithId tableWithId = metaService.getTable(
            tso(), GetTableRequest.builder().tableId(MAPPER.idTo(tableId)).build()
        ).getTableDefinitionWithId();
        Table table = MAPPER.tableFrom(tableWithId, getIndexes(tableWithId, tableWithId.getTableId()));
        table.indexes.forEach($ -> tableIdCache.put($.getTableId(), $));
        return table;
    }

    private synchronized Optional<Table> loadTable(Names names) {
        DingoCommonId schemaId = Parameters.nonNull(
            metaService.getSchemaByName(
                tso(), GetSchemaByNameRequest.builder().schemaName(names.schema).build()
            ).getSchema(),
            "Schema " + names.schema + " not found."
        ).getId();
        TableDefinitionWithId table = Optional.mapOrNull(metaService.getTableByName(
            tso(), GetTableByNameRequest.builder().schemaId(schemaId).tableName(names.table).build()
        ), GetTableByNameResponse::getTableDefinitionWithId);
        if (table == null) {
            return Optional.empty();
        }
        Table result = MAPPER.tableFrom(table, getIndexes(table, table.getTableId()));
        result.indexes.forEach($ -> tableIdCache.put($.getTableId(), $));
        return Optional.of(result);
    }

    private List<TableDefinitionWithId> getIndexes(TableDefinitionWithId tableWithId, DingoCommonId tableId) {
        return metaService.getTables(tso(), GetTablesRequest.builder().tableId(tableId).build())
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

    @SneakyThrows
    private NavigableMap<ComparableByteArray, RangeDistribution> loadDistribution(CommonId tableId) {
        List<io.dingodb.sdk.service.entity.meta.RangeDistribution> ranges;
        Table table = tableIdCache.get(tableId);
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(table.tupleType(), table.keyMapping());
        boolean isOriginalKey = table.getPartitionStrategy().equalsIgnoreCase("HASH");
        if (tableId.type == CommonId.CommonType.TABLE) {
            ranges = metaService.getTableRange(
                tso(), GetTableRangeRequest.builder().tableId(MAPPER.idTo(tableId)).build()
            ).getTableRange().getRangeDistribution();
        } else {
            ranges = metaService.getIndexRange(
                tso(), GetIndexRangeRequest.builder().indexId(MAPPER.idTo(tableId)).build()
            ).getIndexRange().getRangeDistribution();
        }
        NavigableMap<ComparableByteArray, RangeDistribution> result = new TreeMap();
        for (io.dingodb.sdk.service.entity.meta.RangeDistribution range : ranges) {
            RangeDistribution distribution = mapping(range, codec, isOriginalKey);
            result.put(new ComparableByteArray(distribution.getStartKey(), 1), distribution);
        }
        return result;
    }

    private RangeDistribution mapping(
        io.dingodb.sdk.service.entity.meta.RangeDistribution rangeDistribution,
        KeyValueCodec codec,
        boolean isOriginalKey
    ) {
        byte[] startKey = rangeDistribution.getRange().getStartKey();
        byte[] endKey = rangeDistribution.getRange().getEndKey();
        return RangeDistribution.builder()
            .id(MAPPER.idFrom(rangeDistribution.getId()))
            .startKey(startKey)
            .endKey(endKey)
            .start(codec.decodeKeyPrefix(isOriginalKey ? Arrays.copyOf(startKey, startKey.length) : startKey))
            .end(codec.decodeKeyPrefix(isOriginalKey ? Arrays.copyOf(endKey, endKey.length) : endKey))
            .build();
    }

    public synchronized void invalidTable(String schema, String table) {
        tableNameCache.invalidate(new Names(schema, table));
    }

    public synchronized void invalidTable(CommonId tableId) {
        tableIdCache.invalidate(tableId);
    }

    public synchronized void invalidDistribution(CommonId id) {
        distributionCache.invalidate(id);
    }

    @SneakyThrows
    public Table getTable(String schema, String table) {
        return tableNameCache.get(new Names(schema, table)).ifAbsent(() -> invalidTable(schema, table)).orNull();
    }

    @SneakyThrows
    public Table getTable(CommonId tableId) {
        return tableIdCache.get(tableId);
    }

    @SneakyThrows
    public NavigableMap<ComparableByteArray, RangeDistribution> getRangeDistribution(CommonId id) {
        return distributionCache.get(id);
    }

}
