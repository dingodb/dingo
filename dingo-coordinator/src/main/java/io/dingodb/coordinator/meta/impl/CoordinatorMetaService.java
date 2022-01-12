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

package io.dingodb.coordinator.meta.impl;

import com.alipay.sofa.jraft.util.Requires;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.coordinator.config.ServerConfiguration;
import io.dingodb.meta.Location;
import io.dingodb.meta.LocationGroup;
import io.dingodb.meta.MetaService;
import io.dingodb.store.row.client.FutureHelper;
import io.dingodb.store.row.serialization.Serializer;
import io.dingodb.store.row.serialization.Serializers;
import io.dingodb.store.row.util.Maps;
import io.dingodb.store.row.util.MetadataKeyHelper;
import io.dingodb.store.row.util.StringBuilderHelper;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Slf4j
public class CoordinatorMetaService implements MetaService {

    private static final CoordinatorMetaService INSTANCE = new CoordinatorMetaService();
    private final ConcurrentMap<String, byte[]> tableMap = Maps.newConcurrentMap();
    private final ServerConfiguration configuration = ServerConfiguration.INSTANCE;
    private final Serializer serializer = Serializers.getDefault();
    private TableMetaAdaptorImpl metaAdaptor;

    public CoordinatorMetaService() {
    }

    public static CoordinatorMetaService instance() {
        return INSTANCE;
    }

    public void init(TableMetaAdaptorImpl metaAdaptor) {
        this.metaAdaptor = metaAdaptor;
    }

    @Override
    public void init(@Nullable Map<String, Object> props) {

    }

    @Override
    public void createTable(@Nonnull String tableName, @Nonnull TableDefinition tableDefinition) {
        if (!isCoordinatorLeader()) {
            throw new RuntimeException("Only coordinator leader create table.");
        }
        if (this.metaAdaptor.get(tableName) == null) {
            generateTableKey(tableName);
            final byte[] bytes = this.serializer.writeObject(tableDefinition);
            this.metaAdaptor.save(tableName, bytes);
        }
    }

    @Override
    public byte[] getIndexId(@Nonnull String tableName) {
        return new byte[0];
    }

    @Override
    public byte[] getTableKey(@Nonnull String tableName) {
        byte[] bytes = tableMap.get(tableName);
        if (bytes == null) {
            generateTableKey(tableName);
        }
        return tableMap.get(tableName);
    }

    @Override
    public boolean dropTable(@Nonnull String tableName) {
        if (!isCoordinatorLeader()) {
            throw new RuntimeException("Only coordinator leader drop table.");
        }
        return helperGet(this.metaAdaptor.delete(tableName));
    }

    @Override
    public LocationGroup getLocationGroup(String name) {
        Requires.requireNonNull(name, "table name");
        return this.metaAdaptor.rangeLocationGroup().floorEntry(getTableKey(name)).getValue();
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
        Map<String, TableDefinition> map = Maps.newHashMap();
        tableMap.keySet().forEach(name ->
            map.put(name, serializer.readObject(helperGet(this.metaAdaptor.get(name)), TableDefinition.class)));
        return map;
    }

    @Override
    public Map<String, Location> getPartLocations(String name) {
        Map<String, Location> result = Maps.newHashMap();
        List<Location> locations = this.metaAdaptor.rangeLocationGroup().get(getTableKey(name)).getLocations();
        for (int i = 0; i < locations.size(); i++) {
            result.put(String.valueOf(i), locations.get(i));
        }
        return result;
    }

    @Override
    public Location currentLocation() {
        return new Location(configuration.instanceHost(), configuration.port(), "");
    }

    @Override
    public void clear() {

    }

    private void generateTableKey(String name) {
        Long tableId = helperGet(metaAdaptor.getTableId(name));
        Requires.requireNonNull(tableId, "table id");

        char prefix = MetadataKeyHelper.generatePrefix(tableId);
        String fixedLenId = String.format("%08d", tableId);
        String key = StringBuilderHelper.get().append(prefix).append("00").append(fixedLenId).toString();
        tableMap.putIfAbsent(name, serializer.writeObject(key));
    }

    private <T> T helperGet(CompletableFuture<T> future) {
        return FutureHelper.get(future, configuration.futureTimeMillis());
    }

    private boolean isCoordinatorLeader() {
        // todo
        return true;
    }
}
