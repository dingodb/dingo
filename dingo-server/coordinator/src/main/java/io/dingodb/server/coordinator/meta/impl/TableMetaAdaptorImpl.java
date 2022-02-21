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

package io.dingodb.server.coordinator.meta.impl;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.meta.Location;
import io.dingodb.meta.LocationGroup;
import io.dingodb.raft.util.BytesUtil;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.server.coordinator.GeneralId;
import io.dingodb.server.coordinator.app.AppView;
import io.dingodb.server.coordinator.app.impl.RegionApp;
import io.dingodb.server.coordinator.app.impl.RegionView;
import io.dingodb.server.coordinator.meta.ScheduleMetaAdaptor;
import io.dingodb.server.coordinator.meta.TableMetaAdaptor;
import io.dingodb.server.coordinator.resource.impl.ExecutorView;
import io.dingodb.server.coordinator.store.AsyncKeyValueStore;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class TableMetaAdaptorImpl extends AbstractMetaAdaptor implements TableMetaAdaptor {

    private static final String DATA_DIR = "";

    private final AsyncKeyValueStore store;
    private final ScheduleMetaAdaptor scheduleMetaAdaptor;

    public TableMetaAdaptorImpl(AsyncKeyValueStore store, ScheduleMetaAdaptor scheduleMetaAdaptor) {
        this.store = store;
        this.scheduleMetaAdaptor = scheduleMetaAdaptor;
    }

    @Override
    public CompletableFuture<Boolean> save(String tableName, byte[] bytes) {
        return store.put(GeneralId.tableDefinitionOf(getTableId(tableName).join(), tableName).encode(), bytes);
    }

    @Override
    public CompletableFuture<Boolean> delete(String tableName) {
        return store.delete(GeneralId.tableDefinitionOf(getTableId(tableName).join(), tableName).encode());
    }

    @Override
    public CompletableFuture<byte[]> get(String tableName) {
        return store.get(GeneralId.tableDefinitionOf(getTableId(tableName).join(), tableName).encode());
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> getAllDefinition(Function<byte[], T> deserializer) {
        return getAll(GeneralId.tableDefinitionPrefix(), deserializer);
    }

    @Override
    public CompletableFuture<Map<String, Long>> getAllKey() {
        return getAll(GeneralId.tableMetaPrefix(), PrimitiveCodec::readVarLong);
    }

    private <T> CompletableFuture<Map<String, T>> getAll(byte[] prefix, Function<byte[], T> deserializer) {
        return store.scan(prefix)
            .thenApplyAsync(entries ->
                entries.stream().collect(Collectors.toMap(
                    e -> GeneralId.decode(e.getKey()).name(),
                    e -> deserializer.apply(e.getValue())
                ))
            );
    }

    @Override
    public CompletableFuture<Long> getTableId(String tableName) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        byte[] tableIdKey = tableIdKey(tableName).encode();
        store.get(tableIdKey)
            .whenCompleteAsync((r, e) -> {
                if (e == null) {
                    Long id;
                    if (r == null || (id = PrimitiveCodec.readVarLong(r)) == null) {
                        store.increment("tableId".getBytes(StandardCharsets.UTF_8)).whenCompleteAsync((r1, e1) -> {
                            if (e1 == null) {
                                future.complete(r1);
                            } else {
                                future.completeExceptionally(e1);
                            }
                        });
                    } else {
                        future.complete(id);
                    }
                } else {
                    future.completeExceptionally(e);
                }
            });
        future.thenAccept(r -> store.put(tableIdKey, PrimitiveCodec.encodeVarLong(r)));
        return future;
    }

    private GeneralId tableIdKey(String tableName) {
        // todo seq should increment when alter table
        return GeneralId.tableMetaOf(0, tableName);
    }

    @Override
    public NavigableMap<byte[], LocationGroup> rangeLocationGroup() {
        NavigableMap<byte[], LocationGroup> result = new TreeMap<>(BytesUtil.getDefaultByteArrayComparator());
        Map<GeneralId, AppView<?, ?>> map = this.scheduleMetaAdaptor.namespaceView().appViews();
        for (Map.Entry<GeneralId, AppView<?, ?>> entry : map.entrySet()) {
            if (entry.getValue() instanceof RegionView) {
                RegionView view = (RegionView) entry.getValue();
                ExecutorView executorView = this.scheduleMetaAdaptor.executorView(view.leader());
                Endpoint endpoint = executorView.stats().getLocation();
                Location location = new Location(endpoint.getIp(), endpoint.getPort(), DATA_DIR);
                List<Location> locationList = view.nodeResources().stream()
                    .map(id -> this.scheduleMetaAdaptor.namespaceView().<ExecutorView>getResourceView(id))
                    .map(ExecutorView::location)
                    .collect(Collectors.toList());
                LocationGroup locationGroup = new LocationGroup(location, locationList);
                RegionApp regionApp = this.scheduleMetaAdaptor.regionApp(view.app());
                result.put(BytesUtil.nullToEmpty(regionApp.startKey()), locationGroup);
            }
        }
        return result;
    }

    @Override
    public Location currentLocation() {
        return null;
    }
}
