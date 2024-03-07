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

package io.dingodb.meta.local;

import com.google.auto.service.AutoService;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.MetaServiceProvider;
import io.dingodb.meta.TableStatistic;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Partition;
import io.dingodb.meta.entity.Table;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.dingodb.common.CommonId.CommonType.DISTRIBUTION;
import static io.dingodb.common.CommonId.CommonType.SCHEMA;
import static io.dingodb.common.CommonId.CommonType.TABLE;


public class LocalMetaService implements MetaService {

    public static final CommonId ROOT_ID = new CommonId(SCHEMA, 1, 1);
    public static final String ROOT_NAME = "LOCAL_ROOT";
    public static final LocalMetaService ROOT = new LocalMetaService(ROOT_ID, ROOT_NAME);
    private static final NavigableMap<CommonId, LocalMetaService> metaServices = new ConcurrentSkipListMap<>();
    private static final NavigableMap<CommonId, TableDefinition> tableDefinitions = new ConcurrentSkipListMap<>();
    private static final Map<CommonId, NavigableMap<ComparableByteArray, RangeDistribution>> distributions = new ConcurrentSkipListMap<>();
    private static final AtomicInteger metaServiceSeq = new AtomicInteger(1);
    private static final AtomicInteger tableSeq = new AtomicInteger(1);
    private static final AtomicInteger distributionSeq = new AtomicInteger(1);
    private static final AtomicLong autoIncrementId = new AtomicLong(1);
    private static Location location;
    private static NavigableMap<ComparableByteArray, RangeDistribution> defaultDistributions;
    private final CommonId id;
    private final String name;

    private LocalMetaService(CommonId id, String name) {
        this.id = id;
        this.name = name;
    }

    public static void clear() {
        metaServices.clear();
        tableDefinitions.clear();
    }

    public static void setLocation(Location location) {
        LocalMetaService.location = location;
    }

    @Override
    public CommonId id() {
        return id;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void createSubMetaService(String name) {
        CommonId newId = new CommonId(id.type, id.seq, metaServiceSeq.incrementAndGet());
        metaServices.put(newId, new LocalMetaService(newId, name));
    }

    @Override
    public Map<String, io.dingodb.meta.MetaService> getSubMetaServices() {
        return metaServices.subMap(
                CommonId.prefix(SCHEMA, id.seq), true,
                CommonId.prefix(SCHEMA, id.seq + 1), false
            ).values().stream()
            .collect(Collectors.toMap(LocalMetaService::name, __ -> __));
    }

    @Override
    public io.dingodb.meta.MetaService getSubMetaService(String name) {
        return getSubMetaServices().get(name);
    }

    @Override
    public boolean dropSubMetaService(String name) {
        metaServices.remove(getSubMetaService(name).id());
        return true;
    }

    @Override
    public void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition) {
        CommonId tableId = new CommonId(TABLE, id.seq, tableSeq.incrementAndGet());
        tableDefinitions.put(tableId, tableDefinition);
        if (tableDefinition.getPartDefinition() != null) {
            KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(tableDefinition);
            PartitionDetailDefinition start = null;
            for (PartitionDetailDefinition detail : tableDefinition.getPartDefinition().getDetails()) {
                createDistribution(tableId, start, detail, codec);
                start = detail;
            }
            createDistribution(tableId, start, null, codec);
        }
    }

    @Override
    public void createTables(@NonNull TableDefinition tableDefinition,
                             @NonNull List<TableDefinition> indexTableDefinitions) {
        CommonId tableId = new CommonId(TABLE, id.seq, tableSeq.incrementAndGet());
        tableDefinitions.put(tableId, tableDefinition);
        if (tableDefinition.getPartDefinition() != null) {
            KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(tableDefinition);
            PartitionDetailDefinition start = null;
            for (PartitionDetailDefinition detail : tableDefinition.getPartDefinition().getDetails()) {
                if (detail.getOperand().length == 0) {
                    continue;
                }
                createDistribution(tableId, start, detail, codec);
                start = detail;
            }
            createDistribution(tableId, start, null, codec);
        }
    }

    @Override
    public boolean dropTable(@NonNull String tableName) {
        tableName = tableName.toUpperCase();
        CommonId tableId = getTable(tableName).getTableId();
        if (tableId != null) {
            tableDefinitions.remove(tableId);
            return true;
        }
        return false;
    }

    public boolean dropTables(@NonNull Collection<CommonId> tableIds) {
        tableIds.forEach(tableDefinitions::remove);
        return true;
    }

    @Override
    public boolean truncateTable(@NonNull String tableName) {
        TableDefinition tableDefinition = tableDefinitions.get(getTableId(tableName));
        if (tableDefinition != null) {
            dropTable(tableName);
        }
        createTables(tableDefinition, Collections.emptyList());
        return true;
    }

    public CommonId getTableId(@NonNull String tableName) {
        String tableNameU = tableName.toUpperCase();
        return tableDefinitions.subMap(
                CommonId.prefix(TABLE, id.seq), true,
                CommonId.prefix(TABLE, id.seq + 1), false
            ).entrySet().stream()
            .filter(e -> e.getValue().getName().equals(tableNameU))
            .findAny().map(Map.Entry::getKey).orElse(null);
    }

    public Map<String, TableDefinition> getTableDefinitions() {
        return tableDefinitions.subMap(
                CommonId.prefix(TABLE, id.seq), true,
                CommonId.prefix(TABLE, id.seq + 1), false
            ).values().stream()
            .collect(Collectors.toMap(TableDefinition::getName, __ -> __));
    }

    public TableDefinition getTableDefinition(@NonNull String tableName) {
        tableName = tableName.toUpperCase();
        return getTableDefinitions().get(tableName);
    }

    public TableDefinition getTableDefinition(@NonNull CommonId id) {
        return tableDefinitions.get(id);
    }

    public List<TableDefinition> getTableDefinitions(@NonNull String name) {
        //TODO:
        return Collections.singletonList(getTableDefinition(name));
    }

    @Override
    public Table getTable(String tableName) {
        CommonId tableId = getTableId(tableName);
        if (tableId == null) {
            return null;
        }
        return toTable(tableId, getTableDefinition(tableName));
    }

    @Override
    public Table getTable(CommonId tableId) {
        return toTable(tableId, getTableDefinition(tableId));
    }

    @Override
    public Set<Table> getTables() {
        return tableDefinitions.subMap(
                CommonId.prefix(TABLE, id.seq), true,
                CommonId.prefix(TABLE, id.seq + 1), false
            ).entrySet().stream()
            .map($ -> toTable($.getKey(), $.getValue()))
            .collect(Collectors.toSet());
    }

    private Table toTable(CommonId tableId, TableDefinition definition) {
        List<Column> columns = definition.getColumns().stream().map($ -> Column.builder()
            .autoIncrement($.isAutoIncrement())
            .defaultValueExpr($.getDefaultValue())
            .name($.getName())
            .sqlTypeName($.getTypeName())
            .elementTypeName($.getElementType())
            .precision($.getPrecision())
            .primaryKeyIndex($.getPrimary())
            .scale($.getScale())
            .type($.getType())
            .state($.getState())
            .build()
        ).collect(Collectors.toList());
        return Table.builder()
            .tableId(tableId)
            .columns(columns)
            .replica(definition.getReplica())
            .partitionStrategy(definition.getPartDefinition().getFuncName())
            .partitions(definition.getPartDefinition().getDetails().stream()
                .map($ -> Partition.builder().id(CommonId.EMPTY_PARTITION).operand($.getOperand()).build())
                .collect(Collectors.toList())
            ).engine(definition.getEngine())
            .version(definition.getVersion())
            .properties(definition.getProperties())
            .indexes(Collections.emptyList())
            .tableType(definition.getTableType())
            .createSql(definition.getCreateSql())
            .name(definition.getName())
            .build();
    }

    @Override
    public NavigableMap<ComparableByteArray, RangeDistribution> getRangeDistribution(CommonId id) {
        return Parameters.cleanNull(distributions.get(id), defaultDistributions);
    }

    @Override
    public Location currentLocation() {
        return location;
    }

    @Override
    public void createIndex(String tableName, @NonNull List<Index> indexList) {
        TableDefinition td = getTableDefinition(tableName);
    }

    @Override
    public void dropIndex(String tableName, String indexName) {

    }

    public void createDistribution(
        CommonId tableId, PartitionDetailDefinition start, PartitionDetailDefinition end, KeyValueCodec codec
    ) {
        byte[] startKey;
        byte[] endKey;
        if (start == null) {
            startKey = ByteArrayUtils.EMPTY_BYTES;
        } else {
            startKey = codec.encodeKeyPrefix(start.getOperand(), start.getOperand().length);
        }
        if (end == null) {
            endKey = ByteArrayUtils.MAX;
        } else {
            endKey = codec.encodeKeyPrefix(end.getOperand(), end.getOperand().length);
        }
        addRangeDistributions(
            tableId,
            startKey,
            endKey,
            Optional.mapOrNull(start, PartitionDetailDefinition::getOperand),
            Optional.mapOrNull(end, PartitionDetailDefinition::getOperand)
        );
    }

    public void addRangeDistributions(CommonId id, byte[] start, byte[] end) {
        addRangeDistributions(id, start, end, null, null);
    }

    public void addRangeDistributions(CommonId id, byte[] startKey, byte[] endKey, Object[] start, Object[] end) {
        distributions.compute(id, (k, v) -> {
            if (v == null) {
                v = new TreeMap<>();
            }
            v.put(
                new ComparableByteArray(startKey),
                RangeDistribution.builder()
                    .id(new CommonId(DISTRIBUTION, id.seq, v.size() + 1))
                    .startKey(startKey)
                    .endKey(endKey)
                    .start(start)
                    .end(end)
                    .build()
            );
            return v;
        });
    }

    public void setRangeDistributions(NavigableMap<ComparableByteArray, RangeDistribution> distributions) {
        defaultDistributions = distributions;
    }

    @Override
    public TableStatistic getTableStatistic(@NonNull String tableName) {
        return () -> 30000d;
    }

    @Override
    public TableStatistic getTableStatistic(@NonNull CommonId tableId) {
        return () -> 30000d;
    }

    @Override
    public Long getAutoIncrement(CommonId tableId) {
        return autoIncrementId.getAndIncrement();
    }

    @Override
    public Long getNextAutoIncrement(CommonId tableId) {
        return 1L;
    }

    @Override
    public void updateAutoIncrement(CommonId tableId, long autoIncrementId) {

    }

    @AutoService(MetaServiceProvider.class)
    public static class Provider implements MetaServiceProvider {
        @Override
        public io.dingodb.meta.MetaService root() {
            return ROOT;
        }
    }
}
