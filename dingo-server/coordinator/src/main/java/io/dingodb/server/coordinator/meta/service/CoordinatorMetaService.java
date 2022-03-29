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

package io.dingodb.server.coordinator.meta.service;

import io.dingodb.common.config.DingoOptions;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.Location;
import io.dingodb.meta.LocationGroup;
import io.dingodb.meta.MetaService;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.raft.util.Requires;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.coordinator.meta.TableMetaAdaptor;
import io.dingodb.server.protocol.ServerError;
import io.dingodb.store.row.client.FutureHelper;
import io.dingodb.store.row.serialization.Serializer;
import io.dingodb.store.row.serialization.Serializers;
import io.dingodb.store.row.util.Maps;
import io.dingodb.store.row.util.StringBuilderHelper;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Slf4j
public class CoordinatorMetaService implements MetaService, MetaServiceApi {

    private static final CoordinatorMetaService INSTANCE = new CoordinatorMetaService();
    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private final ConcurrentMap<String, byte[]> tableKeyMap = Maps.newConcurrentMap();
    private final ConcurrentMap<String, TableDefinition> tableDefinitionMap = Maps.newConcurrentMap();
    private final Serializer serializer = Serializers.getDefault();
    private TableMetaAdaptor metaAdaptor;

    public CoordinatorMetaService() {
    }

    public static CoordinatorMetaService instance() {
        return INSTANCE;
    }

    public void init(TableMetaAdaptor metaAdaptor) {
        this.metaAdaptor = metaAdaptor;
        metaAdaptor.getAllDefinition(bytes -> this.serializer.readObject(bytes, TableDefinition.class))
            .thenAcceptAsync(tableDefinitionMap::putAll);
        metaAdaptor.getAllKey().thenAcceptAsync(map -> map.forEach(this::generateTableKey));
        netService.apiRegistry().register(MetaServiceApi.class, this);
    }

    @Override
    public void init(@Nullable Map<String, Object> props) {

    }

    @Override
    public String getName() {
        return "DINGO";
    }

    @Override
    public void createTable(@Nonnull String tableName, @Nonnull TableDefinition tableDefinition) {
        if (!isCoordinatorLeader()) {
            throw new RuntimeException("Only coordinator leader create table.");
        }
        this.tableDefinitionMap.compute(tableName, (name, def) -> {
            if (def != null) {
                ServerError.TABLE_EXIST.throwFormatError(tableName);
            }
            final byte[] bytes = this.serializer.writeObject(tableDefinition);
            if (helperGet(this.metaAdaptor.save(tableName, bytes))) {
                return tableDefinition;
            }
            throw ServerError.UNKNOWN.formatAsException("Create table failed.");
        });
        log.info("Create table [{}] finish.", tableName);
    }

    @Override
    public byte[] getIndexId(@Nonnull String tableName) {
        return new byte[0];
    }

    @Override
    public byte[] getTableKey(@Nonnull String tableName) {
        byte[] bytes = tableKeyMap.get(tableName);
        if (bytes == null) {
            generateTableKey(tableName);
        }
        return tableKeyMap.get(tableName);
    }

    @Override
    public boolean dropTable(@Nonnull String tableName) {
        if (!isCoordinatorLeader()) {
            throw new RuntimeException("Only coordinator leader drop table.");
        }

        if (tableDefinitionMap.containsKey(tableName)) {
            CompletableFuture<Boolean> future = this.metaAdaptor.delete(tableName);
            future.thenRun(() -> tableDefinitionMap.remove(tableName));
            return helperGet(future);
        }

        return false;
    }

    @Override
    public LocationGroup getLocationGroup(String name) {
        Requires.requireNonNull(name, "table name");
        return this.metaAdaptor.rangeLocationGroup().floorEntry(getTableKey(name)).getValue();
    }

    @Override
    public TableDefinition getTableDefinition(String name) {
        return tableDefinitionMap.get(name);
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
        return new HashMap<>(tableDefinitionMap);
    }

    @Override
    public Map<String, Location> getPartLocations(String name) {
        return Collections.singletonMap("0", getLocationGroup(name).getLeader());
    }

    @Override
    public Location currentLocation() {
        return new Location(DingoOptions.instance().getIp(), DingoOptions.instance().getExchange().getPort(), "");
    }

    @Override
    public void clear() {
    }

    private void generateTableKey(String name) {
        Long tableId = helperGet(metaAdaptor.getTableId(name));
        Requires.requireNonNull(tableId, "table id");

        generateTableKey(name, tableId);
    }

    private void generateTableKey(String name, Long tableId) {
        char prefix = Utils.oddEvenHashAlphabetOrNumber(tableId);
        String fixedLenId = String.format("%08d", tableId);
        String key = StringBuilderHelper.get().append(prefix).append("00").append(fixedLenId).toString();
        tableKeyMap.putIfAbsent(name, key.getBytes(StandardCharsets.UTF_8));
    }

    private <T> T helperGet(CompletableFuture<T> future) {
        return FutureHelper.get(future, DingoOptions.instance().getCliOptions().getTimeoutMs());
    }

    private boolean isCoordinatorLeader() {
        // todo
        return true;
    }
}
