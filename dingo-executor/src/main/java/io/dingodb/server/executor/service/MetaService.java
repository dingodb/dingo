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

package io.dingodb.server.executor.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.Part;
import io.dingodb.meta.RangeDistribution;
import io.dingodb.meta.TableStatistic;
import io.dingodb.sdk.service.connector.MetaServiceConnector;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import io.dingodb.server.executor.Configuration;
import io.dingodb.server.executor.common.DingoCommonId;
import io.dingodb.server.executor.common.Mapping;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.server.executor.common.Mapping.mapping;

public class MetaService implements io.dingodb.meta.MetaService {

    public static final MetaService ROOT = new MetaService(
        new MetaServiceClient(MetaServiceConnector.getMetaServiceConnector(Configuration.coordinators())));

    @AutoService(io.dingodb.meta.MetaServiceProvider.class)
    public static class MetaServiceProvider implements io.dingodb.meta.MetaServiceProvider {
        @Override
        public io.dingodb.meta.MetaService root() {
            return ROOT;
        }
    }

    public static CommonId getParentSchemaId(CommonId tableId) {
        return new CommonId((byte) DingoCommonId.EntityType.ENTITY_TYPE_SCHEMA.getCode(), 0, tableId.domain);
    }

    protected final MetaServiceClient metaServiceClient;

    public MetaService(MetaServiceClient metaServiceClient) {
        this.metaServiceClient = metaServiceClient;
    }

    @Override
    public CommonId id() {
        return null;
    }

    @Override
    public String name() {
        return metaServiceClient.name().toUpperCase();
    }

    @Override
    public void createSubMetaService(String name) {
        metaServiceClient.createSubMetaService(name);
    }

    @Override
    public Map<String, io.dingodb.meta.MetaService> getSubMetaServices() {
        return metaServiceClient.getSubMetaServices().values().stream()
            .map(MetaService::new)
            .collect(Collectors.toMap(MetaService::name, Function.identity()));
    }

    @Override
    public io.dingodb.meta.MetaService getSubMetaService(String name) {
        return new MetaService(metaServiceClient.getSubMetaService(name));
    }

    public MetaService getSubMetaService(CommonId id) {
        return new MetaService(metaServiceClient.getSubMetaService(mapping(id)));
    }

    @Override
    public boolean dropSubMetaService(String name) {
        return metaServiceClient.dropSubMetaService(name);
    }

    @Override
    public void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition) {
        metaServiceClient.createTable(tableName, mapping(tableDefinition));
    }

    @Override
    public boolean dropTable(@NonNull String tableName) {
        return metaServiceClient.dropTable(tableName);
    }

    @Override
    public CommonId getTableId(@NonNull String tableName) {
        return mapping(metaServiceClient.getTableId(tableName));
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
        return metaServiceClient.getTableDefinitions()
            .values()
            .stream()
            .map(Mapping::mapping)
            .collect(Collectors.toMap(TableDefinition::getName, Function.identity()));
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull String name) {
        return mapping(metaServiceClient.getTableDefinition(name));
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull CommonId id) {
        return mapping(metaServiceClient.getTableDefinition(mapping(id)));
    }

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, Part> getParts(String tableName) {
        return null;
    }

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, Part> getParts(CommonId id) {
        return null;
    }

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> getRangeDistribution(CommonId id) {
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> result = new TreeMap<>();
        NavigableMap<io.dingodb.sdk.common.utils.ByteArrayUtils.ComparableByteArray, io.dingodb.sdk.common.table.RangeDistribution> distribution =
            metaServiceClient.getRangeDistribution(mapping(id));
        for (Map.Entry<io.dingodb.sdk.common.utils.ByteArrayUtils.ComparableByteArray, io.dingodb.sdk.common.table.RangeDistribution> entry : distribution.entrySet()) {
            result.put(new ByteArrayUtils.ComparableByteArray(entry.getKey().getBytes()), mapping(entry.getValue()));
        }
        return result;
    }

    @Override
    public Location currentLocation() {
        return DingoConfiguration.location();
    }

    @Override
    public void createIndex(String tableName, List<Index> indexList) {

    }

    @Override
    public void dropIndex(String tableName, String indexName) {

    }

    @Override
    public <T> T getTableProxy(Class<T> clazz, CommonId tableId) {
        return null;
    }

    @Override
    public TableStatistic getTableStatistic(@NonNull String tableName) {
        return new io.dingodb.meta.TableStatistic() {

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

}
