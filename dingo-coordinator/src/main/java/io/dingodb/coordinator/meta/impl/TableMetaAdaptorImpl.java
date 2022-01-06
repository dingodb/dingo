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
import io.dingodb.coordinator.meta.TableMetaAdaptor;
import io.dingodb.coordinator.store.AsyncKeyValueStore;
import io.dingodb.net.Location;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class TableMetaAdaptorImpl implements TableMetaAdaptor {

    private final AsyncKeyValueStore store;

    public TableMetaAdaptorImpl(AsyncKeyValueStore store) {
        this.store = store;
    }

    @Override
    public CompletableFuture<?> save(
        String tableName,
        byte[] bytes
    ) {
        return store.put(tableName.getBytes(StandardCharsets.UTF_8), bytes);
    }

    @Override
    public CompletableFuture<?> delete(String tableName) {
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
            .whenComplete((r, e) -> {
                if (e == null) {
                    Long id;
                    if ((id = PrimitiveCodec.readVarLong(r)) == null) {
                        store.increment("tableId".getBytes(StandardCharsets.UTF_8)).whenComplete((r1, e1) -> {
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
        return future;
    }

    private String tableIdKey(String tableName) {
        return String.format("%s-id", tableName);
    }

    @Override
    public Map<byte[], Object> rangeLocationGroup() {
        return null;
    }

    @Override
    public Location currentLocation() {
        return null;
    }
}
