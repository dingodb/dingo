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

package io.dingodb.store.proxy.service;

import com.google.auto.service.AutoService;
import com.google.common.collect.Iterators;
import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.Coprocessor;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.vector.VectorSearchResponse;
import io.dingodb.meta.MetaService;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.sdk.common.codec.CodecUtils;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.index.IndexParameter;
import io.dingodb.sdk.common.index.VectorIndexParameter;
import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.serial.schema.LongSchema;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.service.ChannelProvider;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.RangeWithOptions;
import io.dingodb.sdk.service.entity.common.ValueType;
import io.dingodb.sdk.service.entity.common.Vector;
import io.dingodb.sdk.service.entity.common.VectorSearchParameter;
import io.dingodb.sdk.service.entity.common.VectorSearchParameter.SearchNest;
import io.dingodb.sdk.service.entity.common.VectorTableData;
import io.dingodb.sdk.service.entity.common.VectorWithDistance;
import io.dingodb.sdk.service.entity.common.VectorWithId;
import io.dingodb.sdk.service.entity.index.VectorAddRequest;
import io.dingodb.sdk.service.entity.index.VectorDeleteRequest;
import io.dingodb.sdk.service.entity.index.VectorSearchRequest;
import io.dingodb.sdk.service.entity.index.VectorWithDistanceResult;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.store.KvBatchCompareAndSetRequest;
import io.dingodb.sdk.service.entity.store.KvBatchDeleteRequest;
import io.dingodb.sdk.service.entity.store.KvBatchGetRequest;
import io.dingodb.sdk.service.entity.store.KvDeleteRangeRequest;
import io.dingodb.sdk.service.entity.store.KvGetRequest;
import io.dingodb.sdk.service.entity.store.KvPutIfAbsentRequest;
import io.dingodb.sdk.service.entity.store.KvPutRequest;
import io.dingodb.store.proxy.service.CodecService.KeyValueCodec;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;

import static io.dingodb.sdk.common.utils.ByteArrayUtils.equal;
import static io.dingodb.store.proxy.common.Mapping.mapping;
import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;
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

    private final Set<Location> coordinators;
    private final MetaService metaService;


    private StoreService() {
        String coordinators = DingoConfiguration.instance().find("coordinators", String.class);
//        metaService = new MetaServiceClient(coordinators);
        this.coordinators = Services.parse(coordinators);
        metaService = MetaService.root();
    }

    //
    // Store service.
    //

    @Override
    public StoreInstance getInstance(@NonNull CommonId tableId, CommonId regionId) {
        return new StoreInstance(tableId, regionId);
    }

    @Override
    public StoreInstance getInstance(@NonNull CommonId tableId, CommonId regionId, TableDefinition tableDefinition) {
        return new StoreInstance(tableId, regionId, tableDefinition);
    }

    public io.dingodb.sdk.service.IndexService indexService(CommonId tableId, CommonId regionId) {
        return Services.indexRegionService(coordinators, MAPPER.idTo(tableId), MAPPER.idTo(regionId), 30);
    }

    public io.dingodb.sdk.service.IndexService indexService(
        io.dingodb.sdk.service.entity.meta.DingoCommonId tableId,
        io.dingodb.sdk.service.entity.meta.DingoCommonId regionId
    ) {
        return Services.indexRegionService(coordinators, tableId, regionId, 30);
    }

    public io.dingodb.sdk.service.IndexService indexService(CommonId tableId, CommonId regionId, int retry) {
        return Services.indexRegionService(coordinators, MAPPER.idTo(tableId), MAPPER.idTo(regionId), retry);
    }

    public io.dingodb.sdk.service.IndexService indexService(
        io.dingodb.sdk.service.entity.meta.DingoCommonId tableId,
        io.dingodb.sdk.service.entity.meta.DingoCommonId regionId,
        int retry
    ) {
        return Services.indexRegionService(coordinators, tableId, regionId, retry);
    }

    public io.dingodb.sdk.service.StoreService storeService(CommonId tableId, CommonId regionId) {
        return Services.storeRegionService(coordinators, MAPPER.idTo(tableId), MAPPER.idTo(regionId), 30);
    }

    public io.dingodb.sdk.service.StoreService storeService(
        io.dingodb.sdk.service.entity.meta.DingoCommonId tableId,
        io.dingodb.sdk.service.entity.meta.DingoCommonId regionId
    ) {
        return Services.storeRegionService(coordinators, tableId, regionId, 30);
    }

    public io.dingodb.sdk.service.StoreService storeService(CommonId tableId, CommonId regionId, int retry) {
        return Services.storeRegionService(coordinators, MAPPER.idTo(tableId), MAPPER.idTo(regionId), retry);
    }

    public io.dingodb.sdk.service.StoreService storeService(
        io.dingodb.sdk.service.entity.meta.DingoCommonId tableId,
        io.dingodb.sdk.service.entity.meta.DingoCommonId regionId,
        int retry
    ) {
        return Services.storeRegionService(coordinators, tableId, regionId, retry);
    }

    class StoreInstance implements io.dingodb.store.api.StoreInstance {

        private final DingoCommonId storeTableId;
        private final DingoCommonId storeRegionId;

        private final CommonId tableId;
        private final CommonId partitionId;
        private final CommonId regionId;
        private final DingoSchema<Long> schema = new LongSchema(0);

        private Table table;
        private KeyValueCodec tableCodec;
        private Map<CommonId, TableDefinition> tableDefinitionMap;
        private Map<DingoCommonId, Table> tableMap;

        @Delegate
        private final TransactionStoreInstance transactionStoreInstance;
        private final io.dingodb.sdk.service.StoreService storeService;

        public StoreInstance(CommonId tableId, CommonId regionId) {
            this.storeTableId = MAPPER.idTo(tableId);
            this.storeRegionId = MAPPER.idTo(regionId);
            this.tableId = tableId;
            this.regionId = regionId;
            this.partitionId = new CommonId(CommonId.CommonType.PARTITION, tableId.seq, regionId.domain);
            this.tableDefinitionMap = metaService.getTableIndexDefinitions(tableId);
            this.table = mapping(metaService.getTableDefinition(tableId));
            this.tableCodec = (KeyValueCodec) CodecService.getDefault().createKeyValueCodec(mapping(table));
            this.tableMap = new HashMap<>();
            this.tableDefinitionMap.forEach((k, v) -> {
                io.dingodb.store.proxy.common.TableDefinition table = mapping(v);
                table.setProperties(table.getProperties());
                tableMap.put(MAPPER.idTo(k), table);
            });
            this.storeService = storeService(tableId, regionId);
            this.transactionStoreInstance = new TransactionStoreInstance(storeService,partitionId);
        }

        public StoreInstance(CommonId tableId, CommonId regionId, TableDefinition tableDefinition) {
            this.storeTableId = MAPPER.idTo(tableId);
            this.storeRegionId = MAPPER.idTo(regionId);
            this.tableId = tableId;
            this.regionId = regionId;
            this.partitionId = new CommonId(CommonId.CommonType.PARTITION, tableId.seq, regionId.domain);
            this.tableDefinitionMap = metaService.getTableIndexDefinitions(tableId);
            this.table = mapping(tableDefinition);
            this.tableCodec = (KeyValueCodec) CodecService.getDefault().createKeyValueCodec(mapping(table));
            this.tableMap = new HashMap<>();
            this.tableDefinitionMap.forEach((k, v) -> {
                io.dingodb.store.proxy.common.TableDefinition table = mapping(v);
                table.setProperties(table.getProperties());
                tableMap.put(MAPPER.idTo(k), table);
            });
            this.storeService = storeService(tableId, regionId);
            this.transactionStoreInstance = new TransactionStoreInstance(storeService,partitionId);
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
            return storeService.kvPutIfAbsent(
                KvPutIfAbsentRequest.builder().kv(MAPPER.kvTo(setId(row))).build()
            ).isKeyState();
        }

        @Override
        public boolean insertWithIndex(Object[] record) {
            return insert(tableCodec.encode(record));
        }

        @Override
        public boolean insertIndex(Object[] record) {
            for (Map.Entry<DingoCommonId, Table> entry : tableMap.entrySet()) {
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
            KeyValue oldKv = setId(tableCodec.encode(oldRecord));
            KeyValue newKv = setId(tableCodec.encode(newRecord));
            return storeService.kvBatchCompareAndSet(
                    KvBatchCompareAndSetRequest.builder()
                        .kvs(Collections.singletonList(MAPPER.kvTo(newKv)))
                        .expectValues(Collections.singletonList(oldKv.getValue())).build()
                ).getKeyStates().get(0);
        }

        @Override
        public boolean deleteWithIndex(Object[] key) {
            byte[] bytes = this.tableCodec.encodeKey(key);
            return storeService.kvBatchDelete(KvBatchDeleteRequest.builder().keys(singletonList(setId(bytes))).build())
                .getKeyStates().get(0);
        }

        @Override
        public boolean deleteIndex(Object[] record) {
            record = (Object[]) tableCodec.type.convertTo(record, DingoConverter.INSTANCE);
            for (Map.Entry<DingoCommonId, Table> entry : tableMap.entrySet()) {
                DingoCommonId indexId = entry.getKey();
                Table index = entry.getValue();
                Boolean result = false;
                if (index.getIndexParameter().getIndexType().equals(IndexParameter.IndexType.INDEX_TYPE_VECTOR)) {
                    Column primaryCol = index.getColumns().get(0);
                    schema.setIsKey(true);
                    schema.setAllowNull(false);
                    long id = Long.parseLong(String.valueOf(record[table.getColumnIndex(primaryCol.getName())]));

                    DingoKeyValueCodec vectorCodec = new DingoKeyValueCodec(0l, singletonList(schema));
                    DingoCommonId regionId;
                    try {
                        CommonId commonId = MAPPER.idFrom(indexId);
                        NavigableMap<ByteArrayUtils.ComparableByteArray, io.dingodb.common.partition.RangeDistribution> distribution = MetaService.root().getIndexRangeDistribution(commonId, mapping(index));
                        PartitionService ps = PartitionService.getService(
                            Optional.ofNullable(index.getPartition())
                                .map(Partition::getFuncName)
                                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                        regionId = MAPPER.idTo(ps.calcPartId(vectorCodec.encodeKey(new Object[]{id}),distribution));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    result = indexService(indexId, regionId).vectorDelete(
                        VectorDeleteRequest.builder().ids(singletonList(id)).build()
                    ).getKeyStates().get(0);
                } else {
                    List<DingoSchema> schemas = index.getKeyColumns().stream()
                        .map(k -> CodecUtils.createSchemaForColumn(k, table.getColumnIndex(k.getName())))
                        .collect(Collectors.toList());
                    DingoKeyValueCodec indexCodec = new DingoKeyValueCodec(0L, schemas);
                    try {
                        byte[] bytes = indexCodec.encodeKey(record);
                        CommonId commonId = MAPPER.idFrom(indexId);
                        NavigableMap<ByteArrayUtils.ComparableByteArray, io.dingodb.common.partition.RangeDistribution> distribution = MetaService.root().getIndexRangeDistribution(commonId, mapping(index));
                        PartitionService ps = PartitionService.getService(
                            Optional.ofNullable(index.getPartition())
                                .map(Partition::getFuncName)
                                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                        DingoCommonId regionId = MAPPER.idTo(ps.calcPartId(bytes, distribution));
                        result = storeService(indexId, regionId).kvBatchDelete(KvBatchDeleteRequest.builder()
                                .keys(singletonList(indexCodec.resetPrefix(bytes, regionId.getParentEntityId())))
                                .build()).getKeyStates().get(0);
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
            for (Map.Entry<DingoCommonId, Table> entry : tableMap.entrySet()) {
                DingoCommonId indexId = entry.getKey();
                Table index = entry.getValue();
                boolean result = false;
                if (index.getIndexParameter().getIndexType().equals(IndexParameter.IndexType.INDEX_TYPE_VECTOR)) {
                    Column primaryKey = index.getColumns().get(0);
                    schema.setIsKey(true);
                    schema.setAllowNull(false);
                    DingoKeyValueCodec vectorCodec = new DingoKeyValueCodec(0l, singletonList(schema));

                    long newLongId = Long.parseLong(
                        String.valueOf(newRecord[table.getColumnIndex(primaryKey.getName())])
                    );
                    long oldLongId = Long.parseLong(
                        String.valueOf(oldRecord[table.getColumnIndex(primaryKey.getName())])
                    );
                    if (newLongId != oldLongId) {
                        try {
                            CommonId commonId = MAPPER.idFrom(indexId);
                            NavigableMap<ByteArrayUtils.ComparableByteArray, io.dingodb.common.partition.RangeDistribution> distribution = MetaService.root().getIndexRangeDistribution(commonId, mapping(index));
                            PartitionService ps = PartitionService.getService(
                                Optional.ofNullable(index.getPartition())
                                    .map(Partition::getFuncName)
                                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                            DingoCommonId regionId = MAPPER.idTo(ps.calcPartId(vectorCodec.encodeKey(new Object[]{oldLongId}), distribution));
                            indexService(indexId, regionId).vectorDelete(
                                VectorDeleteRequest.builder().ids(singletonList(oldLongId)).build()
                            );
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } else {
                    List<DingoSchema> schemas = new ArrayList<>();
                    for (Column column : index.getColumns()) {
                        schemas.add(CodecUtils.createSchemaForColumn(column, table.getColumnIndex(column.getName())));
                    }
                    DingoKeyValueCodec indexCodec = new DingoKeyValueCodec(0l, schemas);
                    try {
                        io.dingodb.sdk.common.KeyValue newKv = indexCodec.encode(newRecord);
                        io.dingodb.sdk.common.KeyValue oldKv = indexCodec.encode(oldRecord);
                        if (!equal(newKv.getKey(), oldKv.getKey())) {
                            CommonId commonId = MAPPER.idFrom(indexId);
                            NavigableMap<ByteArrayUtils.ComparableByteArray, io.dingodb.common.partition.RangeDistribution> distribution = MetaService.root().getIndexRangeDistribution(commonId, mapping(index));
                            PartitionService ps = PartitionService.getService(
                                Optional.ofNullable(index.getPartition())
                                    .map(Partition::getFuncName)
                                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                            DingoCommonId regionId = MAPPER.idTo(ps.calcPartId(oldKv.getKey(), distribution));
                            storeService(indexId, regionId).kvBatchDelete(KvBatchDeleteRequest.builder()
                                .keys(singletonList(indexCodec.resetPrefix(oldKv.getKey(), regionId.getParentEntityId())))
                                .build()
                            );
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
                return storeService.kvBatchCompareAndSet(
                    KvBatchCompareAndSetRequest.builder()
                        .kvs(singletonList(MAPPER.kvTo(row)))
                        .expectValues(singletonList(old.getValue()))
                        .build()
                ).getKeyStates().get(0);
            }
            return false;
        }

        @Override
        public boolean delete(byte[] key) {
            return storeService.kvBatchDelete(
                KvBatchDeleteRequest.builder().keys(singletonList(key)).build()
            ).getKeyStates().get(0);
        }

        @Override
        public long delete(Range range) {
            return storeService.kvDeleteRange(KvDeleteRangeRequest.builder()
                .range(RangeWithOptions.builder()
                    .withStart(range.withStart)
                    .withEnd(range.withEnd)
                    .range(new io.dingodb.sdk.service.entity.common.Range(setId(range.start), setId(range.end)))
                    .build())
                .build()).getDeleteCount();
        }

        @Override
        public KeyValue get(byte[] key) {
            return new KeyValue(key, storeService.kvGet(KvGetRequest.builder().key(setId(key)).build()).getValue());
        }

        @Override
        public List<KeyValue> get(List<byte[]> keys) {
            keys = keys.stream().map(this::setId).collect(Collectors.toList());
            return storeService.kvBatchGet(KvBatchGetRequest.builder().keys(keys).build())
                .getKvs().stream().map(MAPPER::kvFrom).collect(Collectors.toList());
        }

        @Override
        public Iterator<KeyValue> scan(Range range) {
            ChannelProvider channelProvider = Services.regionChannelProvider(
                coordinators, MAPPER.idTo(tableId), MAPPER.idTo(regionId)
            );
            return Iterators.transform(new ScanIterator(
                regionId,
                channelProvider,
                MAPPER.rangeTo(partitionId.seq, range),
                null,
                30
            ), MAPPER::kvFrom);
        }

        @Override
        public Iterator<KeyValue> scan(Range range, Coprocessor coprocessor) {
            ChannelProvider channelProvider = Services.regionChannelProvider(
                coordinators, MAPPER.idTo(tableId), MAPPER.idTo(regionId)
            );
            return Iterators.transform(new ScanIterator(
                regionId,
                channelProvider,
                MAPPER.rangeTo(partitionId.seq, range),
                MAPPER.coprocessorTo(coprocessor),
                30
            ), MAPPER::kvFrom);
        }

        @Override
        public List<VectorSearchResponse> vectorSearch(
            CommonId indexId, Float[] floatArray, int topN, Map<String, Object> parameterMap
        ) {

            List<VectorWithId> vectors = new ArrayList<>();
            Table indexTable = tableMap.get(MAPPER.idTo(indexId));
            IndexParameter indexParameter = indexTable.getIndexParameter();
            Map<String, String> properties = indexTable.getProperties();

            Vector vector = Vector.builder()
                .dimension(Integer.valueOf(properties.get("dimension")))
                .floatValues(Arrays.asList(floatArray))
                .valueType(ValueType.FLOAT)
                .build();

            VectorWithId vectorWithId = VectorWithId.builder().vector(vector).build();
            vectors.add(vectorWithId);

            SearchNest search = getSearch(indexParameter.getVectorIndexParameter().getVectorIndexType(),
                parameterMap);
            VectorSearchParameter parameter = VectorSearchParameter.builder()
                .topN(topN)
                .search(search)
                .build();
            List<VectorWithDistanceResult> results = indexService(indexId, regionId).vectorSearch(
                VectorSearchRequest.builder()
                    .vectorWithIds(vectors)
                    .parameter(parameter)
                    .build()
            ).getBatchResults();

            List<VectorSearchResponse> vectorSearchResponseList = new ArrayList<>();
            // Add all keys and distances
            for (VectorWithDistanceResult vectorWithDistanceResult : results) {
                List<VectorWithDistance> withDistance = vectorWithDistanceResult.getVectorWithDistances();
                if (withDistance == null || withDistance.isEmpty()) {
                    continue;
                }
                for (VectorWithDistance vectorWithDistance : withDistance) {
                    VectorSearchResponse response = new VectorSearchResponse();
                    response.setKey(vectorWithDistance.getVectorWithId().getTableData().getTableKey());
                    response.setDistance(vectorWithDistance.getDistance());
                    response.setVectorId(vectorWithDistance.getVectorWithId().getId());
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

            Object[] newRecord = (Object[]) tableCodec.type.convertTo(record, DingoConverter.INSTANCE);
            DingoKeyValueCodec indexCodec = new DingoKeyValueCodec(0L, schemas);
            io.dingodb.sdk.common.KeyValue keyValue;
            try {
                keyValue = indexCodec.encode(newRecord);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            CommonId commonId = MAPPER.idFrom(indexId);
            NavigableMap<ByteArrayUtils.ComparableByteArray, io.dingodb.common.partition.RangeDistribution> distribution = MetaService.root().getIndexRangeDistribution(commonId, mapping(index));
            PartitionService ps = PartitionService.getService(
                Optional.ofNullable(index.getPartition())
                    .map(Partition::getFuncName)
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
            DingoCommonId regionId = MAPPER.idTo(ps.calcPartId(keyValue.getKey(), distribution));

            io.dingodb.sdk.service.entity.common.KeyValue kv = io.dingodb.sdk.service.entity.common.KeyValue.builder()
                .key(indexCodec.resetPrefix(keyValue.getKey(), regionId.getParentEntityId()))
                .value(keyValue.getValue())
                .build();
            storeService(indexId, regionId).kvPut(KvPutRequest.builder().kv(kv).build());
            return true;
        }

        private void vectorAdd(Object[] record, Table table,
                               KeyValueCodec tableCodec,
                               DingoCommonId indexId,
                               Table index) {
            Column primaryKey = index.getColumns().get(0);
            schema.setIsKey(true);
            schema.setAllowNull(primaryKey.isNullable());

            long longId = Long.parseLong(String.valueOf(record[table.getColumnIndex(primaryKey.getName())]));

            DingoKeyValueCodec vectorCodec = new DingoKeyValueCodec(0L, singletonList(schema));
            DingoCommonId regionId;
            try {
                CommonId commonId = MAPPER.idFrom(indexId);
                NavigableMap<ByteArrayUtils.ComparableByteArray, io.dingodb.common.partition.RangeDistribution> distribution = MetaService.root().getIndexRangeDistribution(commonId, mapping(index));
                PartitionService ps
                    = PartitionService.getService(index.getPartition().getFuncName());
                regionId = MAPPER.idTo(ps.calcPartId(vectorCodec.encodeKey(new Object[]{longId}), distribution));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            Column value = index.getColumns().get(1);
            Vector vector;
            if (value.getElementType().equalsIgnoreCase("FLOAT")) {
                List<Float> values = (List<Float>) record[table.getColumnIndex(value.getName())];
                vector = Vector.builder().floatValues(values).valueType(ValueType.FLOAT).build();
            } else {
                List<byte[]> values = (List<byte[]>) record[table.getColumnIndex(value.getName())];
                vector = Vector.builder().binaryValues(values).valueType(ValueType.UINT8).build();
            }
            VectorTableData tableData;
            KeyValue keyValue = tableCodec.encode(record);
            tableData = VectorTableData.builder().tableKey(keyValue.getKey()).build();
            VectorWithId vectorWithId = VectorWithId.builder().id(longId).vector(vector).tableData(tableData).build();
            indexService(indexId, regionId).vectorAdd(
                VectorAddRequest.builder().vectors(singletonList(vectorWithId)).build()
            );
        }

        private SearchNest getSearch(
            VectorIndexParameter.VectorIndexType indexType, Map<String, Object> parameterMap
        ) {
            Object o;
            switch (indexType) {
                case VECTOR_INDEX_TYPE_NONE:
                    return null;
                case VECTOR_INDEX_TYPE_DISKANN:
                    return SearchNest.Diskann.builder().build();
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
                    return SearchNest.IvfFlat.builder().nprobe(nprobe).parallelOnQueries(parallel).build();
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
                    return SearchNest.IvfPq.builder().nprobe(np).parallelOnQueries(parallels).recallNum(recallNum).build();
                case VECTOR_INDEX_TYPE_HNSW:
                    int efSearch = 10;
                    o = parameterMap.get("efSearch");
                    if (o != null) {
                        efSearch = ((Number) o).intValue();
                    }

                    return SearchNest.Hnsw.builder().efSearch(efSearch).build();
                case VECTOR_INDEX_TYPE_FLAT:
                default: {
                    int parallelOnQueries = 10;
                    o = parameterMap.get("parallelOnQueries");
                    if (o != null) {
                        parallelOnQueries = ((Number) o).intValue();
                    }

                    return SearchNest.Flat.builder().parallelOnQueries(parallelOnQueries).build();
                }
            }
        }


    }

}
