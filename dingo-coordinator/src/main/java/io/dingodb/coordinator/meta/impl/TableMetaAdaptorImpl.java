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

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.coordinator.GeneralId;
import io.dingodb.coordinator.app.AppView;
import io.dingodb.coordinator.app.impl.RegionApp;
import io.dingodb.coordinator.app.impl.RegionView;
import io.dingodb.coordinator.meta.TableMetaAdaptor;
import io.dingodb.coordinator.resource.impl.ExecutorView;
import io.dingodb.coordinator.store.AsyncKeyValueStore;
import io.dingodb.meta.Location;
import io.dingodb.meta.LocationGroup;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class TableMetaAdaptorImpl implements TableMetaAdaptor {

    private final AsyncKeyValueStore store;
    private final MetaStoreImpl metaStore;

    public TableMetaAdaptorImpl(AsyncKeyValueStore store, MetaStoreImpl metaStore) {
        this.store = store;
        this.metaStore = metaStore;
    }

    @Override
    public CompletableFuture<Boolean> save(
        String tableName,
        byte[] bytes
    ) {
        return store.put(tableName.getBytes(StandardCharsets.UTF_8), bytes);
    }

    @Override
    public CompletableFuture<Boolean> delete(String tableName) {
        return store.delete(tableName.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public CompletableFuture<byte[]> get(String tableName) {
        return store.get(tableName.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public CompletableFuture<Long> getTableId(String tableName) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        store.get(tableIdKey(tableName).getBytes(StandardCharsets.UTF_8))
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
        future.thenAccept(r -> store.put(
            tableIdKey(tableName).getBytes(StandardCharsets.UTF_8), PrimitiveCodec.encodeVarLong(r)));
        return future;
    }

    private String tableIdKey(String tableName) {
        return String.format("%s-id", tableName);
    }

    @Override
    public NavigableMap<byte[], LocationGroup> rangeLocationGroup() {
        NavigableMap<byte[], LocationGroup> result = new TreeMap<>();
        Map<GeneralId, AppView<?, ?>> map = this.metaStore.namespaceView().appViews();
        for (Map.Entry<GeneralId, AppView<?, ?>> entry : map.entrySet()) {
            if (entry.getValue() instanceof RegionView) {
                RegionView view = (RegionView) entry.getValue();
                ExecutorView executorView = this.metaStore.executorView(view.leader());
                Location location = executorView.location();
                List<Location> locationList = view.nodeResources().stream()
                    .map(id -> this.metaStore.namespaceView().<ExecutorView>getResourceView(id))
                    .map(ExecutorView::location)
                    .collect(Collectors.toList());
                LocationGroup locationGroup = new LocationGroup(location, locationList);
                RegionApp regionApp = this.metaStore.regionApp(view.app());
                result.put(regionApp.startKey(), locationGroup);
            }
        }
        return result;
    }

    @Override
    public Location currentLocation() {
        return null;
    }
}
