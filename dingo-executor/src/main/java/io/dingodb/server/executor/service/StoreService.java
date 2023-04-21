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
import io.dingodb.common.store.KeyValue;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.Range;
import io.dingodb.sdk.common.RangeWithOptions;
import io.dingodb.sdk.service.store.StoreServiceClient;
import io.dingodb.server.executor.common.Mapping;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.server.executor.common.Mapping.mapping;

public class StoreService implements io.dingodb.store.api.StoreService {
    public static final StoreService DEFAULT_INSTANCE = new StoreService();

    @AutoService(io.dingodb.store.api.StoreServiceProvider.class)
    public static class StoreServiceProvider implements io.dingodb.store.api.StoreServiceProvider {
        @Override
        public io.dingodb.store.api.StoreService get() {
            return DEFAULT_INSTANCE;
        }
    }

    private final StoreServiceClient storeService;

    private StoreService() {
        storeService = new StoreServiceClient(MetaService.ROOT.metaServiceClient);
    }

    @Override
    public StoreInstance getInstance(@NonNull CommonId tableId, CommonId regionId) {
        return new StoreInstance(storeService, tableId, regionId);
    }

    @Override
    public void deleteInstance(CommonId id) {

    }

    class StoreInstance implements io.dingodb.store.api.StoreInstance {

        private final StoreServiceClient storeService;
        private final DingoCommonId tableId;
        private final DingoCommonId regionId;

        public StoreInstance(StoreServiceClient storeService, CommonId tableId, CommonId regionId) {
            this.storeService = storeService;
            this.tableId = mapping(tableId);
            this.regionId = mapping(regionId);
        }

        @Override
        public boolean upsertKeyValue(KeyValue keyValue) {
            return storeService.kvPut(tableId, regionId, mapping(keyValue));
        }

        @Override
        public boolean upsertKeyValue(List<KeyValue> keyValues) {
            return storeService.kvBatchPut(
                tableId,
                regionId,
                keyValues.stream().map(Mapping::mapping).collect(Collectors.toList()));
        }

        @Override
        public boolean delete(byte[] primaryKey) {
            return delete(Collections.singletonList(primaryKey));
        }

        @Override
        public boolean delete(List<byte[]> primaryKeys) {
            return storeService.kvBatchDelete(tableId, regionId, primaryKeys);
        }

        @Override
        public boolean delete(byte[] startPrimaryKey, byte[] endPrimaryKey) {
            return storeService.kvDeleteRange(
                tableId,
                regionId,
                new RangeWithOptions(new Range(startPrimaryKey, endPrimaryKey), true, false)) > 0;
        }

        @Override
        public KeyValue getKeyValueByPrimaryKey(byte[] primaryKey) {
            return new KeyValue(primaryKey, storeService.kvGet(tableId, regionId, primaryKey));
        }

        @Override
        public List<KeyValue> getKeyValueByPrimaryKeys(List<byte[]> primaryKeys) {
            return storeService.kvBatchGet(tableId, regionId, primaryKeys)
                .stream().map(Mapping::mapping).collect(Collectors.toList());
        }

        @Override
        public Iterator<KeyValue> keyValueScan(byte[] key) {
            return keyValueScan(key, key, true, true);
        }

        @Override
        public Iterator<KeyValue> keyValueScan(byte[] startPrimaryKey, byte[] endPrimaryKey) {
            return keyValueScan(startPrimaryKey, endPrimaryKey, true, false);
        }

        @Override
        public Iterator<KeyValue> keyValueScan(byte[] startPrimaryKey, byte[] endPrimaryKey, boolean includeStart, boolean includeEnd) {
            Iterator<io.dingodb.sdk.common.KeyValue> iterator = storeService.scan(
                tableId,
                regionId,
                new Range(startPrimaryKey, endPrimaryKey),
                includeStart,
                includeEnd);
            return new KeyValueIterator(iterator);
        }

        class KeyValueIterator implements Iterator<KeyValue> {
            private final Iterator<io.dingodb.sdk.common.KeyValue> iterator;

            public KeyValueIterator(Iterator<io.dingodb.sdk.common.KeyValue> iterator) {
                this.iterator = iterator;
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KeyValue next() {
                return mapping(iterator.next());
            }
        }

    }

}
