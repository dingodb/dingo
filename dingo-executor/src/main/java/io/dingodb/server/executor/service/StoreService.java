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
import com.google.common.collect.Iterators;
import io.dingodb.common.CommonId;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.service.store.StoreServiceClient;
import io.dingodb.server.executor.common.Mapping;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.server.executor.common.Mapping.mapping;

@Slf4j
public final class StoreService implements io.dingodb.store.api.StoreService {
    public static final StoreService DEFAULT_INSTANCE = new StoreService();

    @AutoService(io.dingodb.store.api.StoreServiceProvider.class)
    public static final class StoreServiceProvider implements io.dingodb.store.api.StoreServiceProvider {
        @Override
        public io.dingodb.store.api.StoreService get() {
            return DEFAULT_INSTANCE;
        }
    }

    private final StoreServiceClient storeService;

    private StoreService() {
        storeService = new StoreServiceClient(MetaService.ROOT.metaServiceClient);
    }

    //
    // Store service.
    //

    @Override
    public StoreInstance getInstance(@NonNull CommonId tableId, CommonId regionId) {
        return new StoreInstance(storeService, tableId, regionId);
    }

    @Override
    public void deleteInstance(CommonId id) {

    }

    static class StoreInstance implements io.dingodb.store.api.StoreInstance {

        private final StoreServiceClient storeService;
        private final DingoCommonId tableId;
        private final DingoCommonId regionId;

        public StoreInstance(StoreServiceClient storeService, CommonId tableId, CommonId regionId) {
            this.storeService = storeService;
            this.tableId = mapping(tableId);
            this.regionId = mapping(regionId);
        }

        @Override
        public boolean insert(KeyValue row) {
            return storeService.kvPutIfAbsent(tableId, regionId, mapping(row));
        }

        @Override
        public boolean update(KeyValue row, KeyValue old) {
            if (ByteArrayUtils.equal(row.getKey(), old.getKey())) {
                // todo use compare and set
                return storeService.kvPut(tableId, regionId, mapping(row));
            }
            throw new IllegalArgumentException();
        }

        @Override
        public boolean delete(byte[] key) {
            return storeService.kvBatchDelete(tableId, regionId, Collections.singletonList(key)).get(0);
        }

        @Override
        public long delete(Range range) {
            return storeService.kvDeleteRange(tableId, regionId, mapping(range));
        }

        @Override
        public KeyValue get(byte[] key) {
            return new KeyValue(key, storeService.kvGet(tableId, regionId, key));
        }

        @Override
        public List<KeyValue> get(List<byte[]> keys) {
            return storeService.kvBatchGet(tableId, regionId, keys).stream()
                .map(Mapping::mapping).collect(Collectors.toList());
        }

        @Override
        public Iterator<KeyValue> scan(Range range) {
            return Iterators.transform(
                storeService.scan(tableId, regionId, mapping(range).getRange(), range.withStart, range.withEnd),
                Mapping::mapping
            );
        }

        @Override
        public long count(Range range) {
            // todo operator push down
            Iterator<KeyValue> iterator = scan(range);
            long count = 0;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            return count;
        }

    }

}
