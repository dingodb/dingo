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

import com.google.common.collect.Iterators;
import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.Coprocessor;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValueWithExpect;
import io.dingodb.sdk.service.store.StoreServiceClient;
import io.dingodb.server.executor.common.Mapping;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.server.executor.common.Mapping.mapping;
import static java.util.Collections.singletonList;

@Slf4j
public final class StoreService implements io.dingodb.store.api.StoreService {
    public static final StoreService DEFAULT_INSTANCE = new StoreService();

    // @AutoService(io.dingodb.store.api.StoreServiceProvider.class)
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
        private final DingoCommonId storeTableId;
        private final DingoCommonId storeRegionId;

        private final CommonId tableId;
        private final CommonId partitionId;
        private final CommonId regionId;

        public StoreInstance(StoreServiceClient storeService, CommonId tableId, CommonId regionId) {
            this.storeService = storeService;
            this.storeTableId = mapping(tableId);
            this.storeRegionId = mapping(regionId);
            this.tableId = tableId;
            this.regionId = regionId;
            this.partitionId = new CommonId(CommonId.CommonType.PARTITION, tableId.seq, regionId.domain);
        }

        @Override
        public CommonId id() {
            return regionId;
        }

        private byte[] setId(byte[] key) {
            return CodecService.getDefault().setId(key, partitionId);
        }

        private KeyValue setId(KeyValue keyValue) {
            return CodecService.getDefault().setId(keyValue, partitionId);
        }

        @Override
        public boolean insert(KeyValue row) {
            return storeService.kvPutIfAbsent(storeTableId, storeRegionId, mapping(setId(row)));
        }

        @Override
        public boolean update(KeyValue row, KeyValue old) {
            row = setId(row);
            old = setId(old);
            if (ByteArrayUtils.equal(row.getKey(), old.getKey())) {
                return storeService.kvCompareAndSet(
                    storeTableId, storeRegionId, new KeyValueWithExpect(row.getKey(), row.getValue(), old.getValue())
                );
            }
            throw new IllegalArgumentException();
        }

        @Override
        public boolean delete(byte[] key) {
            return storeService.kvBatchDelete(storeTableId, storeRegionId, singletonList(setId(key))).get(0);
        }

        @Override
        public long delete(Range range) {
            range = new Range(setId(range.start), setId(range.end), range.withStart, range.withEnd);
            return storeService.kvDeleteRange(storeTableId, storeRegionId, mapping(range));
        }

        @Override
        public KeyValue get(byte[] key) {
            return new KeyValue(key, storeService.kvGet(storeTableId, storeRegionId, setId(key)));
        }

        @Override
        public List<KeyValue> get(List<byte[]> keys) {
            return storeService.kvBatchGet(
                    storeTableId, storeRegionId, keys.stream().map(this::setId).collect(Collectors.toList())
                ).stream()
                .map(Mapping::mapping).collect(Collectors.toList());
        }

        @Override
        public Iterator<KeyValue> scan(Range range) {
            range = new Range(setId(range.start), setId(range.end), range.withStart, range.withEnd);
            return Iterators.transform(
                storeService.scan(
                    storeTableId,
                    storeRegionId,
                    mapping(range).getRange(),
                    range.withStart,
                    range.withEnd),
                Mapping::mapping
            );
        }

        @Override
        public Iterator<KeyValue> scan(Range range, Coprocessor coprocessor) {
            range = new Range(setId(range.start), setId(range.end), range.withStart, range.withEnd);
            return Iterators.transform(
                storeService.scan(
                    storeTableId,
                    storeRegionId,
                    mapping(range).getRange(),
                    range.withStart,
                    range.withEnd,
                    new io.dingodb.server.executor.common.Coprocessor(coprocessor)),
                Mapping::mapping
            );
        }

        @Override
        public long count(Range range) {
            // todo operator push down
            range = new Range(setId(range.start), setId(range.end), range.withStart, range.withEnd);
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
