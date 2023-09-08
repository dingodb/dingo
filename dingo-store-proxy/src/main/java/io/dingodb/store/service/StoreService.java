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
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.vector.VectorSearchResponse;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValueWithExpect;
import io.dingodb.sdk.common.codec.CodecUtils;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.index.IndexParameter;
import io.dingodb.sdk.common.index.VectorIndexParameter;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.serial.schema.LongSchema;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.vector.Search;
import io.dingodb.sdk.common.vector.SearchDiskAnnParam;
import io.dingodb.sdk.common.vector.SearchFlatParam;
import io.dingodb.sdk.common.vector.SearchHnswParam;
import io.dingodb.sdk.common.vector.SearchIvfFlatParam;
import io.dingodb.sdk.common.vector.SearchIvfPqParam;
import io.dingodb.sdk.common.vector.Vector;
import io.dingodb.sdk.common.vector.VectorSearchParameter;
import io.dingodb.sdk.common.vector.VectorTableData;
import io.dingodb.sdk.common.vector.VectorWithDistance;
import io.dingodb.sdk.common.vector.VectorWithDistanceResult;
import io.dingodb.sdk.common.vector.VectorWithId;
import io.dingodb.sdk.service.index.IndexServiceClient;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import io.dingodb.sdk.service.store.StoreServiceClient;
import io.dingodb.store.common.Mapping;
import io.dingodb.store.service.CodecService.KeyValueCodec;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.dingodb.sdk.common.utils.ByteArrayUtils.POS;
import static io.dingodb.sdk.common.utils.ByteArrayUtils.equal;
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

        private Table table;
        private KeyValueCodec tableCodec;
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
            this.tableCodec = (KeyValueCodec) CodecService.getDefault().createKeyValueCodec(mapping(table));
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
                return insert(tableCodec.encode(record));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean insertIndex(Object[] record) {
            for (Map.Entry<DingoCommonId, Table> entry : tableDefinitionMap.entrySet()) {
                Object[] newRecord = Arrays.copyOf(record, record.length);

                DingoCommonId indexId = entry.getKey();
                Table index = entry.getValue();
                if (index.getIndexParameter().getIndexType().equals(IndexParameter.IndexType.INDEX_TYPE_VECTOR)) {
                    // vector index add
                    vectorAdd(newRecord, table, tableCodec, indexId, index);
                } else {
                    // scalar index insert
                    boolean result = scalarInsert(newRecord, table, indexId, index);
                    if (!result) {
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public boolean updateWithIndex(Object[] newRecord, Object[] oldRecord) {
            KeyValueWithExpect kvExpect;
            try {
                KeyValue oldKv = tableCodec.encode(oldRecord);
                KeyValue newKv = tableCodec.encode(newRecord);
                kvExpect = new KeyValueWithExpect(
                    tableCodec.delegate.resetPrefix(oldKv.getKey(), storeRegionId.parentId()),
                    newKv.getValue(),
                    oldKv.getValue()
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return storeService.kvCompareAndSet(storeTableId, storeRegionId, kvExpect);
        }

        @Override
        public boolean deleteWithIndex(Object[] key) {
            try {
                byte[] bytes = this.tableCodec.encodeKey(key);
                return storeService.kvBatchDelete(storeTableId, storeRegionId, singletonList(setId(bytes))).get(0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean deleteIndex(Object[] record) {
            record = (Object[]) tableCodec.type.convertTo(record, DingoConverter.INSTANCE);
            for (Map.Entry<DingoCommonId, Table> entry : tableDefinitionMap.entrySet()) {
                DingoCommonId indexId = entry.getKey();
                Table index = entry.getValue();
                Boolean result = false;
                if (index.getIndexParameter().getIndexType().equals(IndexParameter.IndexType.INDEX_TYPE_VECTOR)) {
                    Column primaryCol = index.getColumns().get(0);
                    long id = Long.parseLong(String.valueOf(record[table.getColumnIndex(primaryCol.getName())]));

                    DingoKeyValueCodec vectorCodec = new DingoKeyValueCodec(indexId.entityId(), singletonList(schema));
                    DingoCommonId regionId;
                    try {
                        regionId = metaService.getIndexRangeDistribution(
                            indexId,
                            new io.dingodb.sdk.common.utils.ByteArrayUtils.ComparableByteArray(vectorCodec.encodeKey(new Object[]{id}), POS)).getId();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    result = indexService.vectorDelete(indexId, regionId, singletonList(id)).get(0);
                } else {
                    List<DingoSchema> schemas = index.getKeyColumns().stream()
                        .map(k -> CodecUtils.createSchemaForColumn(k, table.getColumnIndex(k.getName())))
                        .collect(Collectors.toList());
                    DingoKeyValueCodec indexCodec = new DingoKeyValueCodec(indexId.entityId(), schemas);
                    try {
                        byte[] bytes = indexCodec.encodeKey(record);
                        DingoCommonId regionId = metaService.getIndexRangeDistribution(
                            indexId,
                            new io.dingodb.sdk.common.utils.ByteArrayUtils.ComparableByteArray(bytes, POS)).getId();
                        result = storeService.kvBatchDelete(indexId, regionId, singletonList(indexCodec.resetPrefix(bytes, regionId.parentId()))).get(0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (!result) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean deleteIndex(Object[] newRecord, Object[] oldRecord) {
            newRecord = (Object[]) tableCodec.type.convertTo(newRecord, DingoConverter.INSTANCE);
            oldRecord = (Object[]) tableCodec.type.convertTo(oldRecord, DingoConverter.INSTANCE);
            for (Map.Entry<DingoCommonId, Table> entry : tableDefinitionMap.entrySet()) {
                DingoCommonId indexId = entry.getKey();
                Table index = entry.getValue();
                boolean result = false;
                if (index.getIndexParameter().getIndexType().equals(IndexParameter.IndexType.INDEX_TYPE_VECTOR)) {
                    Column primaryKey = index.getColumns().get(0);
                    schema.setIsKey(true);
                    DingoKeyValueCodec vectorCodec = new DingoKeyValueCodec(indexId.entityId(), singletonList(schema));

                    long newLongId = Long.parseLong(String.valueOf(newRecord[table.getColumnIndex(primaryKey.getName())]));
                    long oldLongId = Long.parseLong(String.valueOf(oldRecord[table.getColumnIndex(primaryKey.getName())]));
                    if (newLongId != oldLongId) {
                        try {
                            DingoCommonId regionId = metaService.getIndexRangeDistribution(
                                indexId,
                                new io.dingodb.sdk.common.utils.ByteArrayUtils.ComparableByteArray(vectorCodec.encodeKey(new Object[]{oldLongId}), POS)).getId();
                            indexService.vectorDelete(indexId, regionId, singletonList(oldLongId));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } else {
                    List<DingoSchema> schemas = new ArrayList<>();
                    for (Column column : index.getColumns()) {
                        schemas.add(CodecUtils.createSchemaForColumn(column, table.getColumnIndex(column.getName())));
                    }
                    DingoKeyValueCodec indexCodec = new DingoKeyValueCodec(indexId.entityId(), schemas);
                    try {
                        io.dingodb.sdk.common.KeyValue newKv = indexCodec.encode(newRecord);
                        io.dingodb.sdk.common.KeyValue oldKv = indexCodec.encode(oldRecord);
                        if (!equal(newKv.getKey(), oldKv.getKey())) {
                            DingoCommonId regionId = metaService.getIndexRangeDistribution(indexId, new io.dingodb.sdk.common.utils.ByteArrayUtils.ComparableByteArray(oldKv.getKey(), POS)).getId();
                            storeService.kvBatchDelete(indexId, regionId, singletonList(indexCodec.resetPrefix(oldKv.getKey(), regionId.parentId())));
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (!result) {
                    return false;
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
                storeService.scan(storeTableId, storeRegionId, mapping(range).getRange(), range.withStart, range.withEnd, new io.dingodb.store.common.Coprocessor(coprocessor)),
                Mapping::mapping
            );
        }

        @Override
        public List<VectorSearchResponse> vectorSearch(
            CommonId indexId, Float[] floatArray, int topN, Map<String, Object> parameterMap) {
            List<VectorWithId> vectors = new ArrayList<>();
            Table indexTable = tableDefinitionMap.get(mapping(indexId));
            IndexParameter indexParameter = indexTable.getIndexParameter();
            Map<String, String> properties = indexTable.getProperties();

            Vector vector = new Vector(
                Integer.valueOf(properties.get("dimension")),
                Vector.ValueType.FLOAT,
                Arrays.asList(floatArray),
                Collections.emptyList());
            VectorWithId vectorWithId = new VectorWithId(0, vector, null, null);
            vectors.add(vectorWithId);

            Search search = getSearch(indexParameter.getVectorIndexParameter().getVectorIndexType(), parameterMap);

            VectorSearchParameter vectorSearchParameter = new VectorSearchParameter(
                topN,
                false,
                false,
                Collections.emptyList(),
                false,
                search,
                null,
                null,
                null,
                null
            );

            List<VectorWithDistanceResult> results = indexService.vectorSearch(mapping(indexId), mapping(regionId),
                vectors, vectorSearchParameter);

            List<VectorSearchResponse> vectorSearchResponseList = new ArrayList<>();
            // Add all keys and distances
            for (VectorWithDistanceResult vectorWithDistanceResult : results) {
                List<VectorWithDistance> withDistance = vectorWithDistanceResult.getWithDistance();
                for (VectorWithDistance vectorWithDistance : withDistance) {
                    VectorSearchResponse response = new VectorSearchResponse();
                    response.setKey(vectorWithDistance.getWithId().getTableData().getKey());
                    response.setDistance(vectorWithDistance.getDistance());
                    vectorSearchResponseList.add(response);
                }
            }

            return vectorSearchResponseList;
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

            DingoCommonId regionId = metaService.getIndexRangeDistribution(indexId,
                new io.dingodb.sdk.common.utils.ByteArrayUtils.ComparableByteArray(keyValue.getKey(), POS)).getId();

            return storeService.kvPut(indexId, regionId, new io.dingodb.sdk.common.KeyValue(
                indexCodec.resetPrefix(keyValue.getKey(), regionId.parentId()), keyValue.getValue()));
        }

        private void vectorAdd(Object[] record, Table table,
                               KeyValueCodec tableCodec,
                               DingoCommonId indexId,
                               Table index) {
            Column primaryKey = index.getColumns().get(0);
            schema.setIsKey(true);
            schema.setAllowNull(primaryKey.isNullable());

            long longId = Long.parseLong(String.valueOf(record[table.getColumnIndex(primaryKey.getName())]));

            DingoKeyValueCodec vectorCodec = new DingoKeyValueCodec(indexId.entityId(), singletonList(schema));
            DingoCommonId regionId;
            try {
                regionId = metaService.getIndexRangeDistribution(indexId, new io.dingodb.sdk.common.utils.ByteArrayUtils
                    .ComparableByteArray(vectorCodec.encodeKey(new Object[]{longId}), POS)).getId();
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
                KeyValue keyValue = tableCodec.encode(record);
                vectorTableData = new VectorTableData(keyValue.getKey(), keyValue.getValue());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            VectorWithId vectorWithId = new VectorWithId(longId, vector, null, vectorTableData);
            indexService.vectorAdd(indexId, regionId, singletonList(vectorWithId), false, false);
        }

        private Search getSearch(VectorIndexParameter.VectorIndexType indexType, Map<String, Object> parameterMap) {
            Search search;
            Object o;
            switch (indexType) {
                case VECTOR_INDEX_TYPE_NONE:
                    return null;
                case VECTOR_INDEX_TYPE_DISKANN:
                    SearchDiskAnnParam searchDiskAnnParam = new SearchDiskAnnParam();
                    search = new Search(searchDiskAnnParam);
                    break;
                case VECTOR_INDEX_TYPE_IVF_FLAT:
                    int nprobe = 10;
                    o = parameterMap.get("nprobe");
                    if (o != null) {
                        nprobe = ((Number) o).intValue();
                    }

                    int parallel = 10;
                    o = parameterMap.get("parallelOnQueries");
                    if (o != null) {
                        parallel = ((Number) o).intValue();
                    }

                    SearchIvfFlatParam searchIvfFlatParam = new SearchIvfFlatParam(nprobe, parallel);
                    search = new Search(searchIvfFlatParam);
                    break;
                case VECTOR_INDEX_TYPE_IVF_PQ:
                    int np = 10;
                    o = parameterMap.get("nprobe");
                    if (o != null) {
                        np = ((Number) o).intValue();
                    }

                    int parallels = 10;
                    o = parameterMap.get("parallelOnQueries");
                    if (o != null) {
                        parallels = ((Number) o).intValue();
                    }

                    int recallNum = 10;
                    o = parameterMap.get("recallNum");
                    if (o != null) {
                        recallNum = ((Number) o).intValue();
                    }

                    SearchIvfPqParam searchIvfPqParam = new SearchIvfPqParam(np, parallels, recallNum);
                    search = new Search(searchIvfPqParam);
                    break;
                case VECTOR_INDEX_TYPE_HNSW:
                    int efSearch = 10;
                    o = parameterMap.get("efSearch");
                    if (o != null) {
                        efSearch = ((Number) o).intValue();
                    }

                    SearchHnswParam searchHnswParam = new SearchHnswParam(efSearch);
                    search = new Search(searchHnswParam);
                    break;
                case VECTOR_INDEX_TYPE_FLAT:
                default: {
                    int parallelOnQueries = 10;
                    o = parameterMap.get("parallelOnQueries");
                    if (o != null) {
                        parallelOnQueries = ((Number) o).intValue();
                    }

                    SearchFlatParam searchFlatParam = new SearchFlatParam(parallelOnQueries);
                    search = new Search(searchFlatParam);
                }
            }
            return search;
        }

    }

}
