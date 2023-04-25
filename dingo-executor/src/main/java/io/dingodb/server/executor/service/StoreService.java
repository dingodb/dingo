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
import io.dingodb.common.util.Optional;
import io.dingodb.meta.RangeDistribution;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.Range;
import io.dingodb.sdk.common.RangeWithOptions;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.service.store.StoreServiceClient;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.server.executor.common.Mapping.mapping;
import static io.dingodb.server.executor.service.MetaService.getParentSchemaId;

@Slf4j
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

    static class StoreInstance implements io.dingodb.store.api.StoreInstance {

        private final StoreServiceClient storeService;
        private final DingoKeyValueCodec codec;
        private final RangeDistribution distribution;

        private final DingoCommonId tableId;
        private final DingoCommonId regionId;

        public StoreInstance(StoreServiceClient storeService, CommonId tableId, CommonId regionId) {
            this.storeService = storeService;
            this.tableId = mapping(tableId);
            this.regionId = mapping(regionId);
            MetaService metaService = MetaService.ROOT.getSubMetaService(getParentSchemaId(tableId));
            distribution = metaService
                .getRangeDistribution(tableId).values().stream().filter(d -> d.id().equals(regionId)).findAny().get();
            Table table = metaService.metaServiceClient.getTableDefinition(mapping(tableId));
            codec = new DingoKeyValueCodec(table.getDingoType(), table.getKeyMapping(), tableId.seq);
        }

        @Override
        public boolean insert(Object[] row) {
            try {
                return storeService.kvPutIfAbsent(tableId, regionId, codec.encode(row));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean update(Object[] row) {
            try {
                return storeService.kvPut(tableId, regionId, codec.encode(row));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Object[] getTupleByPrimaryKey(Object[] primaryKey) {
            try {
                byte[] key = codec.encodeKey(primaryKey);
                byte[] value = storeService.kvGet(tableId, regionId, key);
                if (value == null || value.length == 0) {
                   return null;
                }
                return codec.decode(new io.dingodb.sdk.common.KeyValue(key, storeService.kvGet(tableId, regionId, key)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Iterator<Object[]> tupleScan() {
            return Iterators.transform(storeService.scan(
                tableId,
                regionId,
                new Range(distribution.getStartKey(), distribution.getEndKey()),
                true,
                false
            ), wrap(codec::decode, e -> log.error("Iterator: decode error.", e))::apply);
        }

        @Override
        public Iterator<Object[]> tupleScan(Object[] start, Object[] end, boolean withStart, boolean withEnd) {
            if (start == null && end == null) {
                return tupleScan();
            }
            withEnd = end != null && withEnd;
            byte[] startKey = Optional.mapOrGet(start, wrap(codec::encodeKey), distribution::getStartKey);
            byte[] endKey = Optional.mapOrGet(end, wrap(codec::encodeKey), distribution::getEndKey);
            Iterator<KeyValue> keyValueIterator = storeService.scan(tableId, regionId,
                new Range(startKey, endKey), withStart, withEnd);
            if (keyValueIterator == null) {
                return null;
            }
            return Iterators.transform(
                keyValueIterator,
                wrap(codec::decode, e -> log.error("Iterator: decode error.", e))::apply
            );
        }

        @Override
        public boolean delete(Object[] row) {
            try {
                return storeService.kvBatchDelete(
                    tableId, regionId, Collections.singletonList(codec.encodeKey(row))).get(0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long countDeleteByRange(
            Object[] start, Object[] end, boolean withStart, boolean withEnd, boolean doDelete
        ) {
            withEnd = end != null && withEnd;
            byte[] startKey = Optional.mapOrGet(start, wrap(codec::encodeKey), distribution::getStartKey);
            byte[] endKey = Optional.mapOrGet(end, wrap(codec::encodeKey), distribution::getEndKey);
            if (doDelete) {
                return storeService.kvDeleteRange(
                    tableId, regionId, new RangeWithOptions(new Range(startKey, endKey), withStart, withEnd)
                );
            }
            long count = 0;
            Iterator<KeyValue> iterator = storeService.scan(
                tableId, regionId, new Range(startKey, endKey), true, false);
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            return count;
        }
    }

}
