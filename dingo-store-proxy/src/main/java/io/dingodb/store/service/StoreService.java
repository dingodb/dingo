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

package io.dingodb.store.service;

import com.google.auto.service.AutoService;
import com.google.common.collect.Iterators;
import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.Coprocessor;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValueWithExpect;
import io.dingodb.sdk.common.codec.CodecUtils;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.index.IndexParameter;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.serial.schema.LongSchema;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.vector.Vector;
import io.dingodb.sdk.common.vector.VectorTableData;
import io.dingodb.sdk.common.vector.VectorWithId;
import io.dingodb.sdk.service.index.IndexServiceClient;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import io.dingodb.sdk.service.store.StoreServiceClient;
import io.dingodb.store.common.Mapping;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.dingodb.sdk.common.utils.ByteArrayUtils.POS;
import static io.dingodb.store.common.Mapping.mapping;
import static java.util.Collections.singletonList;

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
    private final MetaServiceClient metaService;

    private StoreService() {
        String coordinators = DingoConfiguration.instance().getStoreOrigin().get("coordinators").toString();
        metaService = new MetaServiceClient(coordinators);
        storeService = new StoreServiceClient(metaService);
    }

    //
    // Store service.
    //

    @Override
    public StoreInstance getInstance(@NonNull CommonId tableId, CommonId regionId) {
        return new StoreInstance(tableId, regionId);
    }

    @Override
    public void deleteInstance(CommonId id) {

    }

    class StoreInstance implements io.dingodb.store.api.StoreInstance {

        private final DingoCommonId storeTableId;
        private final DingoCommonId storeRegionId;
        private final IndexServiceClient indexService;

        private final CommonId tableId;
        private final CommonId partitionId;
        private final CommonId regionId;
        private final DingoSchema<Long> schema = new LongSchema(0);
        private final DingoType dingoType = new LongType(true);

        private Table table;
        private DingoKeyValueCodec tableCodec;
        private Map<DingoCommonId, Table> tableDefinitionMap;

        public StoreInstance(CommonId tableId, CommonId regionId) {
            this.storeTableId = mapping(tableId);
            this.storeRegionId = mapping(regionId);
            this.tableId = tableId;
            this.regionId = regionId;
            this.partitionId = new CommonId(CommonId.CommonType.PARTITION, tableId.seq, regionId.domain);
            this.indexService = new IndexServiceClient(metaService);
            this.tableDefinitionMap = metaService.getTableIndexes(this.storeTableId);
            this.table = metaService.getTableDefinition(storeTableId);
            this.tableCodec = DingoKeyValueCodec.of(storeTableId.entityId(), table);
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
        public boolean insertWithIndex(Object[] record) {
            try {
                io.dingodb.sdk.common.KeyValue keyValue = this.tableCodec.encode(record);
                return storeService.kvPutIfAbsent(
                    storeTableId,
                    storeRegionId,
                    new io.dingodb.sdk.common.KeyValue(tableCodec.resetPrefix(keyValue.getKey(), storeRegionId.parentId()), keyValue.getValue()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean insertIndex(Object[] record) {
            for (Map.Entry<DingoCommonId, Table> entry : tableDefinitionMap.entrySet()) {
                DingoCommonId indexId = entry.getKey();
                Table index = entry.getValue();
                if (index.getIndexParameter().getIndexType().equals(IndexParameter.IndexType.INDEX_TYPE_VECTOR)) {
                    // vector index add
                    vectorAdd(record, table, tableCodec, indexId, index);
                } else {
                    // scalar index insert
                    boolean result = scalarInsert(record, table, indexId, index);
                    if (!result) {
                        return false;
                    }
                }
            }
            return true;
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
                storeService.scan(storeTableId, storeRegionId, mapping(range).getRange(), range.withStart, range.withEnd),
                Mapping::mapping
            );
        }

        @Override
        public Iterator<KeyValue> scan(Range range, Coprocessor coprocessor) {
            range = new Range(setId(range.start), setId(range.end), range.withStart, range.withEnd);
            return Iterators.transform(
                storeService.scan(storeTableId, storeRegionId, mapping(range).getRange(), range.withStart, range.withEnd, new io.dingodb.server.executor.common.Coprocessor(coprocessor)),
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

        private boolean scalarInsert(Object[] record, Table table, DingoCommonId indexId, Table index) {
            List<DingoSchema> schemas = new ArrayList<>();
            for (Column column : index.getColumns()) {
                schemas.add(CodecUtils.createSchemaForColumn(column, table.getColumnIndex(column.getName())));
            }
            DingoKeyValueCodec indexCodec = new DingoKeyValueCodec(indexId.entityId(), schemas);
            io.dingodb.sdk.common.KeyValue keyValue;
            try {
                keyValue = indexCodec.encode(record);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            DingoCommonId regionId = metaService.getIndexRangeDistribution(indexId, new io.dingodb.sdk.common.utils.ByteArrayUtils.ComparableByteArray(keyValue.getKey(), POS)).getId();

            return storeService.kvPut(indexId, regionId, new io.dingodb.sdk.common.KeyValue(indexCodec.resetPrefix(keyValue.getKey(), regionId.parentId()), keyValue.getValue()));
        }

        private void vectorAdd(Object[] record, Table table, DingoKeyValueCodec tableCodec, DingoCommonId indexId, Table index) {
            Column primaryKey = index.getColumns().get(0);
            schema.setIsKey(true);

            long longId = Long.parseLong(String.valueOf(record[table.getColumnIndex(primaryKey.getName())]));

            Object convertId = dingoType.convertTo(new Object[]{longId}, DingoConverter.INSTANCE);
            DingoKeyValueCodec vectorCodec = new DingoKeyValueCodec(indexId.entityId(), Collections.singletonList(schema));
            DingoCommonId regionId;
            try {
                regionId = metaService.getIndexRangeDistribution(indexId, new io.dingodb.sdk.common.utils.ByteArrayUtils.ComparableByteArray(vectorCodec.encodeKey((Object[]) convertId), POS)).getId();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            Column value = index.getColumns().get(1);
            Vector vector;
            if (value.getElementType().equalsIgnoreCase("FLOAT")) {
                List<Float> values = (List<Float>) record[table.getColumnIndex(value.getName())];
                vector = Vector.getFloatInstance(values.size(), values);
            } else {
                List<byte[]> values = (List<byte[]>) record[table.getColumnIndex(value.getName())];
                vector = Vector.getBinaryInstance(values.size(), values);
            }
            VectorTableData vectorTableData;
            try {
                io.dingodb.sdk.common.KeyValue keyValue = tableCodec.encode(record);
                vectorTableData = new VectorTableData(keyValue.getKey(), keyValue.getValue());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            VectorWithId vectorWithId = new VectorWithId(longId, vector, null, vectorTableData);
            indexService.vectorAdd(indexId, regionId, Collections.singletonList(vectorWithId), false, false);
        }

    }

}
