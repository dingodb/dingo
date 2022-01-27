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

package io.dingodb.server.coordinator.meta;

import io.dingodb.meta.Location;
import io.dingodb.meta.LocationGroup;

import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface TableMetaAdaptor extends MetaAdaptor {
    CompletableFuture<Boolean> save(String tableName, byte[] bytes);

    CompletableFuture<Boolean> delete(String tableName);

    CompletableFuture<byte[]> get(String tableName);

    <T> CompletableFuture<Map<String, T>> getAllDefinition(Function<byte[], T> deserializer);

    CompletableFuture<Map<String, Long>> getAllKey();

    CompletableFuture<Long> getTableId(String tableName);

    NavigableMap<byte[], LocationGroup> rangeLocationGroup();

    Location currentLocation();
}
