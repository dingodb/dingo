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
import com.google.common.collect.ImmutableList;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.MetaServiceProvider;
import io.dingodb.meta.Part;
import io.dingodb.meta.RangeDistribution;
import io.dingodb.meta.TableStatistic;
import io.dingodb.net.Channel;
import io.dingodb.server.api.ExecutorApi;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

public class LocalMetaService implements MetaService {

    public static final CommonId ROOT_ID = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.schema, 1, 1);
    public static final String ROOT_NAME = "LOCAL_ROOT";
    public static final LocalMetaService ROOT = new LocalMetaService(ROOT_ID, ROOT_NAME);
    private static final NavigableMap<CommonId, LocalMetaService> metaServices = new ConcurrentSkipListMap<>();
    private static final NavigableMap<CommonId, TableDefinition> tableDefinitions = new ConcurrentSkipListMap<>();
    private static final Map<CommonId, NavigableMap<ComparableByteArray, Part>> parts = new ConcurrentSkipListMap<>();
    private static final Map<CommonId, NavigableMap<ComparableByteArray, RangeDistribution>> distributions = new ConcurrentSkipListMap<>();
    private static final AtomicInteger metaServiceSeq = new AtomicInteger(1);
    private static final AtomicInteger tableSeq = new AtomicInteger(1);
    private static Location location;
    private static NavigableMap<ComparableByteArray, Part> defaultPart;
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
        parts.clear();
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
        CommonId newId = new CommonId(id.type(), id.seq(), metaServiceSeq.incrementAndGet());
        metaServices.put(newId, new LocalMetaService(newId, name));
    }

    @Override
    public Map<String, io.dingodb.meta.MetaService> getSubMetaServices() {
        return metaServices.subMap(
                CommonId.prefix(ID_TYPE.table, id.seq()), true,
                CommonId.prefix(ID_TYPE.table, id.seq() + 1), false
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
        CommonId tableId = new CommonId(ID_TYPE.table, id.seq(), tableSeq.incrementAndGet());
        tableDefinitions.put(tableId, tableDefinition);
    }

    @Override
    public boolean dropTable(@NonNull String tableName) {
        tableName = tableName.toUpperCase();
        tableDefinitions.remove(getTableId(tableName));
        return true;
    }

    @Override
    public CommonId getTableId(@NonNull String tableName) {
        String tableNameU = tableName.toUpperCase();
        return tableDefinitions.subMap(
                CommonId.prefix(ID_TYPE.table, id.seq()), true,
                CommonId.prefix(ID_TYPE.table, id.seq() + 1), false
            ).entrySet().stream()
            .filter(e -> e.getValue().getName().equals(tableNameU))
            .findAny().map(Map.Entry::getKey).orElse(null);
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
        return tableDefinitions.subMap(
                CommonId.prefix(ID_TYPE.table, id.seq()), true,
                CommonId.prefix(ID_TYPE.table, id.seq() + 1), false
            ).values().stream()
            .collect(Collectors.toMap(TableDefinition::getName, __ -> __));
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull String tableName) {
        tableName = tableName.toUpperCase();
        return getTableDefinitions().get(tableName);
    }

    public TableDefinition getTableDefinition(@NonNull CommonId id) {
        return tableDefinitions.get(id);
    }

    @Override
    public NavigableMap<ComparableByteArray, Part> getParts(String tableName) {
        tableName = tableName.toUpperCase();
        return Parameters.cleanNull(parts.get(getTableId(tableName)), defaultPart);
    }

    @Override
    public NavigableMap<ComparableByteArray, Part> getParts(CommonId id) {
        return Parameters.cleanNull(parts.get(id), defaultPart);
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
        indexList.forEach(td::addIndex);
    }

    @Override
    public void dropIndex(String tableName, String indexName) {

    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getTableProxy(@NonNull Class<T> clazz, CommonId tableId) {
        if (clazz.isAssignableFrom(ExecutorApi.class)) {
            return (T) new ExecutorApi() {

                @Override
                public boolean upsertKeyValue(Channel channel, CommonId schema, CommonId tableId, KeyValue row) {
                    return false;
                }

                @Override
                public boolean upsertKeyValue(Channel channel, CommonId schema, CommonId tableId, List<KeyValue> rows) {
                    return false;
                }

                @Override
                public boolean upsertKeyValue(Channel channel, CommonId schema, CommonId tableId, byte[] primaryKey, byte[] row) {
                    return false;
                }

                @Override
                public byte[] getValueByPrimaryKey(Channel channel, CommonId schema, CommonId tableId, byte[] primaryKey) {
                    return new byte[0];
                }

                @Override
                public List<KeyValue> getKeyValueByPrimaryKeys(Channel channel, CommonId schema, CommonId tableId, List<byte[]> primaryKeys) {
                    return null;
                }

                @Override
                public boolean delete(Channel channel, CommonId schema, CommonId tableId, byte[] primaryKey) {
                    return false;
                }

                @Override
                public boolean delete(Channel channel, CommonId schema, CommonId tableId, List<byte[]> primaryKeys) {
                    return false;
                }

                @Override
                public boolean deleteRange(Channel channel, CommonId schema, CommonId tableId, byte[] startPrimaryKey, byte[] endPrimaryKey) {
                    return false;
                }

                @Override
                public List<KeyValue> getKeyValueByRange(Channel channel, CommonId schema, CommonId tableId, byte[] startPrimaryKey, byte[] endPrimaryKey) {
                    return null;
                }

                @Override
                public List<KeyValue> getKeyValueByKeyPrefix(Channel channel, CommonId schema, CommonId tableId, byte[] keyPrefix) {
                    return null;
                }

                @Override
                public List<KeyValue> getAllKeyValue(Channel channel, CommonId schema, CommonId tableId) {
                    return null;
                }

                @Override
                public Future<Object> operator(CommonId tableId, List<byte[]> startPrimaryKey, List<byte[]> endPrimaryKey, byte[] op, boolean readOnly) {
                    return null;
                }

                @Override
                public List<Object[]> select(CommonId tableId, Object[] row, boolean[] hasData) {
                    return ImmutableList.of();
                }
            };
        }
        return null;
    }

    public void setParts(CommonId id, NavigableMap<ComparableByteArray, Part> part) {
        parts.put(id, part);
    }

    public void setParts(NavigableMap<ComparableByteArray, Part> part) {
        defaultPart = part;
    }

    public void addRangeDistributions(CommonId id, byte[] start, byte[] end) {
        distributions.compute(id, (k,v) -> {
            if (v == null) {
                v = new TreeMap<>();
            }
            v.put(
                new ComparableByteArray(start),
                new RangeDistribution(new CommonId(ID_TYPE.table, id.seq(), v.size() + 1) , start, end)
            );
            return v;
        });
    }

    public void setRangeDistributions(NavigableMap<ComparableByteArray, RangeDistribution> distributions) {
        defaultDistributions = distributions;
    }

    @AutoService(MetaServiceProvider.class)
    public static class Provider implements MetaServiceProvider {
        @Override
        public io.dingodb.meta.MetaService root() {
            return ROOT;
        }
    }

    @Override
    public TableStatistic getTableStatistic(@NonNull String tableName) {
        return () -> 30000d;
    }
}
