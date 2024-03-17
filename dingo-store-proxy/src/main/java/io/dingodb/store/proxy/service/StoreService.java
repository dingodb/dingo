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
import io.dingodb.common.CoprocessorV2;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.common.vector.TxnVectorSearchResponse;
import io.dingodb.common.vector.VectorSearchResponse;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.serial.schema.LongSchema;
import io.dingodb.sdk.service.ChannelProvider;
import io.dingodb.sdk.service.IndexService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.RangeWithOptions;
import io.dingodb.sdk.service.entity.common.ValueType;
import io.dingodb.sdk.service.entity.common.Vector;
import io.dingodb.sdk.service.entity.common.VectorFilter;
import io.dingodb.sdk.service.entity.common.VectorFilterType;
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
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import io.dingodb.store.proxy.service.CodecService.KeyValueCodec;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
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
import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;

@Slf4j
public final class StoreService implements io.dingodb.store.api.StoreService {
    public static final StoreService DEFAULT_INSTANCE = new StoreService();
    public static final int RETRY = 60;

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
        this.coordinators = Services.parse(coordinators);
        metaService = MetaService.root();
    }

    //
    // Store service.
    //

    // todo fix
    private class Proxy implements InvocationHandler {

        private final StoreInstance storeInstance;

        private Proxy(StoreInstance storeInstance) {
            this.storeInstance = storeInstance;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(storeInstance, args);
            } catch (Exception e) {
                Throwable throwable = Utils.extractThrowable(e);
                if (throwable instanceof DingoClientException.InvalidRouteTableException) {
                    throw new RegionSplitException(throwable);
                }
                throw throwable;
            }
        }
    }

    @Override
    public io.dingodb.store.api.StoreInstance getInstance(@NonNull CommonId tableId, CommonId regionId) {
        return (io.dingodb.store.api.StoreInstance) java.lang.reflect.Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class[]{io.dingodb.store.api.StoreInstance.class},
            new Proxy(new StoreInstance(tableId, regionId))
        );
    }

    @Override
    public io.dingodb.store.api.StoreInstance getInstance(
        @NonNull CommonId tableId, CommonId regionId, CommonId indexId
    ) {
        return (io.dingodb.store.api.StoreInstance) java.lang.reflect.Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class[]{io.dingodb.store.api.StoreInstance.class},
            new Proxy(new StoreInstance(tableId, regionId, indexId))
        );
    }

    public io.dingodb.sdk.service.IndexService indexService(CommonId tableId, CommonId regionId) {
        return Services.indexRegionService(coordinators, MAPPER.idTo(tableId), MAPPER.idTo(regionId), RETRY);
    }

    public io.dingodb.sdk.service.IndexService indexService(
        io.dingodb.sdk.service.entity.meta.DingoCommonId tableId,
        io.dingodb.sdk.service.entity.meta.DingoCommonId regionId
    ) {
        return Services.indexRegionService(coordinators, tableId, regionId, RETRY);
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
        return Services.storeRegionService(coordinators, regionId.seq, RETRY);
    }

    public io.dingodb.sdk.service.StoreService storeService(
        io.dingodb.sdk.service.entity.meta.DingoCommonId tableId,
        io.dingodb.sdk.service.entity.meta.DingoCommonId regionId
    ) {
        return Services.storeRegionService(coordinators, regionId.getEntityId(), RETRY);
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

        private final Table table;
        private final KeyValueCodec tableCodec;
        private final Map<CommonId, IndexTable> tableMap;

        @Delegate
        private final TransactionStoreInstance transactionStoreInstance;
        private final io.dingodb.sdk.service.StoreService storeService;
        private IndexService indexService;

        public StoreInstance(CommonId tableId, CommonId regionId) {
            this.storeTableId = MAPPER.idTo(tableId);
            this.storeRegionId = MAPPER.idTo(regionId);
            this.tableId = tableId;
            this.regionId = regionId;
            this.partitionId = new CommonId(CommonId.CommonType.PARTITION, tableId.seq, regionId.domain);
            this.table = metaService.getTable(tableId);
            if (table.getIndexes() != null) {
                this.tableMap = table.getIndexes().stream().collect(Collectors.toMap(Table::getTableId, identity()));
            } else {
                this.tableMap = new HashMap<>();
            }
            this.tableCodec = (KeyValueCodec) CodecService.getDefault().createKeyValueCodec(
                table.tupleType(), table.keyMapping()
            );
            this.storeService = storeService(tableId, regionId);
            if (tableId.type == CommonId.CommonType.INDEX && ((IndexTable) table).indexType.isVector) {
                indexService = indexService(tableId, regionId);
            }
            this.transactionStoreInstance = new TransactionStoreInstance(storeService, indexService, partitionId);
        }

        public StoreInstance(CommonId tableId, CommonId regionId, CommonId indexId) {
            this.storeTableId = MAPPER.idTo(tableId);
            this.storeRegionId = MAPPER.idTo(regionId);
            this.tableId = tableId;
            this.regionId = regionId;
            this.partitionId = new CommonId(CommonId.CommonType.PARTITION, tableId.seq, regionId.domain);
            this.table = metaService.getTable(tableId).getIndexes().stream()
                .filter(indexId::equals).findAny()
                .orElseThrow(() -> new RuntimeException("Not found index " + indexId));
            this.tableMap = table.getIndexes().stream().collect(Collectors.toMap(Table::getTableId, identity()));
            this.tableCodec = (KeyValueCodec) CodecService.getDefault().createKeyValueCodec(
                table.tupleType(), table.keyMapping()
            );
            this.storeService = storeService(tableId, regionId);
            this.transactionStoreInstance = new TransactionStoreInstance(storeService, indexService, partitionId);
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
        public boolean insert(long requestTs, KeyValue row) {
            return storeService.kvPutIfAbsent(requestTs,
                KvPutIfAbsentRequest.builder().kv(MAPPER.kvTo(setId(row))).build()
            ).isKeyState();
        }

        @Override
        public boolean insertWithIndex(long requestTs, Object[] record) {
            return insert(requestTs, tableCodec.encode(record));
        }

        @Override
        public boolean insertIndex(long requestTs, Object[] record) {
            for (Map.Entry<CommonId, IndexTable> entry : tableMap.entrySet()) {
                Object[] newRecord = Arrays.copyOf(record, record.length);

                CommonId indexId = entry.getKey();
                IndexTable index = entry.getValue();
                if (index.getIndexType().isVector) {
                    vectorAdd(requestTs, newRecord, table, tableCodec, indexId, index);
                } else {
                    boolean result = scalarInsert(requestTs, newRecord, table, indexId, index);
                    if (!result) {
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public boolean updateWithIndex(long requestTs, Object[] newRecord, Object[] oldRecord) {
            KeyValue oldKv = setId(tableCodec.encode(oldRecord));
            KeyValue newKv = setId(tableCodec.encode(newRecord));
            return storeService.kvBatchCompareAndSet(
                requestTs,
                KvBatchCompareAndSetRequest.builder()
                    .kvs(Collections.singletonList(MAPPER.kvTo(newKv)))
                    .expectValues(Collections.singletonList(oldKv.getValue())).build()
            ).getKeyStates().get(0);
        }

        @Override
        public boolean deleteWithIndex(long requestTs, Object[] key) {
            byte[] bytes = this.tableCodec.encodeKey(key);
            return storeService.kvBatchDelete(
                requestTs,
                KvBatchDeleteRequest.builder().keys(singletonList(setId(bytes))).build()
            ).getKeyStates().get(0);
        }

        @Override
        public boolean deleteIndex(long requestTs, Object[] record) {
            record = (Object[]) tableCodec.type.convertTo(record, DingoConverter.INSTANCE);
            for (Map.Entry<CommonId, IndexTable> entry : tableMap.entrySet()) {
                CommonId indexId = entry.getKey();
                IndexTable index = entry.getValue();
                Boolean result = false;
                if (index.getIndexType().isVector) {
                    Column primaryCol = index.getColumns().get(0);
                    schema.setIsKey(true);
                    schema.setAllowNull(false);
                    long id = Long.parseLong(String.valueOf(record[table.getColumns().indexOf(primaryCol)]));

                    DingoKeyValueCodec vectorCodec = new DingoKeyValueCodec(0L, singletonList(schema));
                    DingoCommonId regionId;
                    NavigableMap<ComparableByteArray, io.dingodb.common.partition.RangeDistribution> distribution =
                        MetaService.root().getRangeDistribution(indexId);
                    PartitionService ps = PartitionService.getService(
                        Optional.ofNullable(index.getPartitionStrategy())
                            .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                    regionId = MAPPER.idTo(ps.calcPartId(vectorCodec.encodeKey(new Object[]{id}), distribution));

                    result = indexService(MAPPER.idTo(indexId), regionId).vectorDelete(
                        requestTs,
                        VectorDeleteRequest.builder().ids(singletonList(id)).build()
                    ).getKeyStates().get(0);
                } else {
                    List<DingoSchema> schemas = index.keyColumns().stream()
                        .map(k -> io.dingodb.store.proxy.service.CodecService.createSchemaForType(
                            k.getType(), table.getColumns().indexOf(k), k.isPrimary()
                        )).collect(Collectors.toList());
                    DingoKeyValueCodec indexCodec = new DingoKeyValueCodec(0L, schemas);
                    byte[] bytes = indexCodec.encodeKey(record);
                    NavigableMap<ComparableByteArray, io.dingodb.common.partition.RangeDistribution> distribution =
                        MetaService.root().getRangeDistribution(indexId);
                    PartitionService ps = PartitionService.getService(
                        Optional.ofNullable(index.getPartitionStrategy())
                            .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                    DingoCommonId regionId = MAPPER.idTo(ps.calcPartId(bytes, distribution));
                    result = storeService(MAPPER.idTo(indexId), regionId).kvBatchDelete(
                        requestTs,
                        KvBatchDeleteRequest
                            .builder()
                            .keys(singletonList(indexCodec.resetPrefix(bytes, regionId.getParentEntityId())))
                            .build()
                    ).getKeyStates().get(0);
                }
                if (!result) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean deleteIndex(long requestTs, Object[] newRecord, Object[] oldRecord) {
            newRecord = (Object[]) tableCodec.type.convertTo(newRecord, DingoConverter.INSTANCE);
            oldRecord = (Object[]) tableCodec.type.convertTo(oldRecord, DingoConverter.INSTANCE);
            for (Map.Entry<CommonId, IndexTable> entry : tableMap.entrySet()) {
                CommonId indexId = entry.getKey();
                IndexTable index = entry.getValue();
                boolean result = false;
                if (index.getIndexType().isVector) {
                    Column primaryKey = index.getColumns().get(0);
                    schema.setIsKey(true);
                    schema.setAllowNull(false);
                    DingoKeyValueCodec vectorCodec = new DingoKeyValueCodec(0L, singletonList(schema));

                    long newLongId = Long.parseLong(
                        String.valueOf(newRecord[table.getColumns().indexOf(primaryKey)])
                    );
                    long oldLongId = Long.parseLong(
                        String.valueOf(oldRecord[table.getColumns().indexOf(primaryKey)])
                    );
                    if (newLongId != oldLongId) {
                        NavigableMap<ComparableByteArray, io.dingodb.common.partition.RangeDistribution> distribution =
                            MetaService.root().getRangeDistribution(indexId);
                        PartitionService ps = PartitionService.getService(
                            Optional.ofNullable(index.getPartitionStrategy())
                                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                        DingoCommonId regionId = MAPPER.idTo(
                            ps.calcPartId(vectorCodec.encodeKey(new Object[]{oldLongId}), distribution)
                        );
                        indexService(MAPPER.idTo(indexId), regionId).vectorDelete(
                            requestTs,
                            VectorDeleteRequest.builder().ids(singletonList(oldLongId)).build()
                        );
                    }
                } else {
                    List<DingoSchema> schemas = new ArrayList<>();
                    for (Column column : index.getColumns()) {
                        schemas.add(io.dingodb.store.proxy.service.CodecService.createSchemaForType(
                            column.getType(), table.getColumns().indexOf(column), column.isPrimary()
                        ));
                    }
                    DingoKeyValueCodec indexCodec = new DingoKeyValueCodec(0L, schemas);
                    io.dingodb.sdk.common.KeyValue newKv = indexCodec.encode(newRecord);
                    io.dingodb.sdk.common.KeyValue oldKv = indexCodec.encode(oldRecord);
                    if (!equal(newKv.getKey(), oldKv.getKey())) {
                        NavigableMap<ComparableByteArray, io.dingodb.common.partition.RangeDistribution> distribution =
                            MetaService.root().getRangeDistribution(indexId);
                        PartitionService ps = PartitionService.getService(
                            Optional.ofNullable(index.getPartitionStrategy())
                                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                        DingoCommonId regionId = MAPPER.idTo(ps.calcPartId(oldKv.getKey(), distribution));
                        storeService(MAPPER.idTo(indexId), regionId).kvBatchDelete(
                            requestTs,
                            KvBatchDeleteRequest
                                .builder()
                                .keys(singletonList(indexCodec.resetPrefix(oldKv.getKey(), regionId.getParentEntityId())))
                                .build()
                        );
                    }
                }
                if (!result) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean update(long requestTs, KeyValue row, KeyValue old) {
            row = setId(row);
            old = setId(old);
            if (ByteArrayUtils.equal(row.getKey(), old.getKey())) {
                return storeService.kvBatchCompareAndSet(
                    requestTs,
                    KvBatchCompareAndSetRequest.builder()
                        .kvs(singletonList(MAPPER.kvTo(row)))
                        .expectValues(singletonList(old.getValue()))
                        .build()
                ).getKeyStates().get(0);
            }
            return false;
        }

        @Override
        public boolean delete(long requestTs, byte[] key) {
            return storeService.kvBatchDelete(
                requestTs,
                KvBatchDeleteRequest.builder().keys(singletonList(key)).build()
            ).getKeyStates().get(0);
        }

        @Override
        public long delete(long requestTs, Range range) {
            return storeService.kvDeleteRange(
                requestTs,
                KvDeleteRangeRequest
                    .builder()
                    .range(RangeWithOptions.builder()
                        .withStart(range.withStart)
                        .withEnd(range.withEnd)
                        .range(io.dingodb.sdk.service.entity.common.Range.builder()
                            .startKey(setId(range.start))
                            .endKey(setId(range.end))
                            .build()
                        )
                        .build()
                    ).build()
            ).getDeleteCount();
        }

        @Override
        public KeyValue get(long requestTs, byte[] key) {
            return new KeyValue(key, storeService.kvGet(
                requestTs, KvGetRequest.builder().key(setId(key)).build()
            ).getValue());
        }

        @Override
        public List<KeyValue> get(long requestTs, List<byte[]> keys) {
            keys = keys.stream().map(this::setId).collect(Collectors.toList());
            return storeService.kvBatchGet(
                requestTs,
                KvBatchGetRequest.builder().keys(keys).build()
            ).getKvs().stream().map(MAPPER::kvFrom).collect(Collectors.toList());
        }

        @Override
        public Iterator<KeyValue> scan(long requestTs, Range range) {
            ChannelProvider channelProvider = Services.regionChannelProvider(
                coordinators, MAPPER.idTo(tableId), MAPPER.idTo(regionId)
            );
            return Iterators.transform(new ScanIterator(
                requestTs,
                regionId,
                channelProvider,
                MAPPER.rangeTo(partitionId.seq, range),
                null,
                RETRY
            ), MAPPER::kvFrom);
        }

        @Override
        public Iterator<KeyValue> scan(long requestTs, Range range, Coprocessor coprocessor) {
            ChannelProvider channelProvider = Services.regionChannelProvider(
                coordinators, MAPPER.idTo(tableId), MAPPER.idTo(regionId)
            );
            return Iterators.transform(new ScanIterator(
                requestTs,
                regionId,
                channelProvider,
                MAPPER.rangeTo(partitionId.seq, range),
                MAPPER.coprocessorTo(coprocessor),
                RETRY
            ), MAPPER::kvFrom);
        }

        @Override
        public Iterator<KeyValue> scan(long requestTs, Range range, CoprocessorV2 coprocessor) {
            ChannelProvider channelProvider = Services.regionChannelProvider(
                coordinators, MAPPER.idTo(tableId), MAPPER.idTo(regionId)
            );
            return Iterators.transform(new ScanIteratorV2(
                requestTs,
                regionId,
                channelProvider,
                MAPPER.rangeTo(partitionId.seq, range),
                MAPPER.coprocessorTo(coprocessor),
                RETRY
            ), MAPPER::kvFrom);
        }

        @Override
        public List<VectorSearchResponse> vectorSearch(
            long requestTs,
            CommonId indexId,
            Float[] floatArray,
            int topN,
            Map<String, Object> parameterMap,
            CoprocessorV2 coprocessor
        ) {

            List<VectorWithId> vectors = new ArrayList<>();
            IndexTable indexTable = tableMap.get(indexId);
            boolean isTxn = indexTable.getEngine().contains("TXN");

            Vector vector = Vector.builder()
                .dimension(Integer.parseInt(indexTable.getProperties().getProperty("dimension")))
                .floatValues(Arrays.asList(floatArray))
                .valueType(ValueType.FLOAT)
                .build();

            VectorWithId vectorWithId = VectorWithId.builder().vector(vector).build();
            vectors.add(vectorWithId);

            SearchNest search = getSearch(indexTable.getIndexType(), parameterMap);
            VectorSearchParameter parameter = VectorSearchParameter.builder()
                .topN(topN)
                .search(search)
                .build();
            if (coprocessor != null) {
                parameter.setVectorCoprocessor(MAPPER.coprocessorTo(coprocessor));
                parameter.setVectorFilter(VectorFilter.TABLE_FILTER);
                parameter.setVectorFilterType(VectorFilterType.QUERY_PRE);
            }
            List<VectorWithDistanceResult> results = indexService(indexId, regionId).vectorSearch(
                requestTs,
                VectorSearchRequest.builder()
                    .vectorWithIds(vectors)
                    .parameter(parameter)
                    .build()
            ).getBatchResults();

            List<VectorSearchResponse> vectorSearchResponseList = new ArrayList<>();
            // Add all keys and distances
            if (results == null) {
                return vectorSearchResponseList;
            }
            for (VectorWithDistanceResult vectorWithDistanceResult : results) {
                if (vectorWithDistanceResult == null) {
                    continue;
                }
                List<VectorWithDistance> withDistance = vectorWithDistanceResult.getVectorWithDistances();
                if (withDistance == null || withDistance.isEmpty()) {
                    continue;
                }
                for (VectorWithDistance vectorWithDistance : withDistance) {
                    VectorSearchResponse response;
                    if (isTxn) {
                        TxnVectorSearchResponse txnResponse = new TxnVectorSearchResponse();
                        txnResponse.setTableKey(vectorWithDistance.getVectorWithId().getTableData().getTableKey());
                        txnResponse.setTableVal(vectorWithDistance.getVectorWithId().getTableData().getTableValue());
                        response = txnResponse;
                    } else {
                        response = new VectorSearchResponse();
                    }
                    response.setFloatValues(vectorWithDistance.getVectorWithId().getVector().getFloatValues());
                    response.setKey(vectorWithDistance.getVectorWithId().getTableData().getTableKey());
                    response.setDistance(vectorWithDistance.getDistance());
                    response.setVectorId(vectorWithDistance.getVectorWithId().getId());
                    vectorSearchResponseList.add(response);
                }
            }

            return vectorSearchResponseList;
        }

        @Override
        public long count(long requestTs, Range range) {
            // todo operator push down
            range = new Range(setId(range.start), setId(range.end), range.withStart, range.withEnd);
            Iterator<KeyValue> iterator = scan(requestTs, range);
            long count = 0;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            return count;
        }

        private boolean scalarInsert(long requestTs, Object[] record, Table table, CommonId indexId, Table index) {
            List<DingoSchema> schemas = new ArrayList<>();
            for (Column column : index.getColumns()) {
                schemas.add(io.dingodb.store.proxy.service.CodecService.createSchemaForType(
                    column.getType(), table.getColumns().indexOf(column), column.isPrimary())
                );
            }

            Object[] newRecord = (Object[]) tableCodec.type.convertTo(record, DingoConverter.INSTANCE);
            DingoKeyValueCodec indexCodec = new DingoKeyValueCodec(0L, schemas);
            io.dingodb.sdk.common.KeyValue keyValue;
            keyValue = indexCodec.encode(newRecord);
            NavigableMap<ComparableByteArray, io.dingodb.common.partition.RangeDistribution> distribution =
                MetaService.root().getRangeDistribution(indexId);
            PartitionService ps = PartitionService.getService(
                Optional.ofNullable(index.getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
            DingoCommonId regionId = MAPPER.idTo(ps.calcPartId(keyValue.getKey(), distribution));

            io.dingodb.sdk.service.entity.common.KeyValue kv = io.dingodb.sdk.service.entity.common.KeyValue.builder()
                .key(indexCodec.resetPrefix(keyValue.getKey(), regionId.getParentEntityId()))
                .value(keyValue.getValue())
                .build();
            storeService(MAPPER.idTo(indexId), regionId).kvPut(requestTs, KvPutRequest.builder().kv(kv).build());
            return true;
        }

        private void vectorAdd(
            long requestTs,
            Object[] record,
            Table table,
            KeyValueCodec tableCodec,
            CommonId indexId,
            Table index
        ) {
            Column primaryKey = index.getColumns().get(0);
            schema.setIsKey(true);
            schema.setAllowNull(primaryKey.isNullable());

            long longId = Long.parseLong(String.valueOf(record[table.getColumns().indexOf(primaryKey)]));

            DingoKeyValueCodec vectorCodec = new DingoKeyValueCodec(0L, singletonList(schema));
            DingoCommonId regionId;
            NavigableMap<ComparableByteArray, io.dingodb.common.partition.RangeDistribution> distribution =
                MetaService.root().getRangeDistribution(indexId);
            PartitionService ps = PartitionService.getService(index.getPartitionStrategy());
            regionId = MAPPER.idTo(ps.calcPartId(vectorCodec.encodeKey(new Object[]{longId}), distribution));

            Column value = index.getColumns().get(1);
            Vector vector;
            if (value.getElementTypeName().equalsIgnoreCase("FLOAT")) {
                List<Float> values = (List<Float>) record[table.getColumns().indexOf(value)];
                vector = Vector.builder().floatValues(values).valueType(ValueType.FLOAT).build();
            } else {
                List<byte[]> values = (List<byte[]>) record[table.getColumns().indexOf(value)];
                vector = Vector.builder().binaryValues(values).valueType(ValueType.UINT8).build();
            }
            VectorTableData tableData;
            KeyValue keyValue = tableCodec.encode(record);
            tableData = VectorTableData.builder().tableKey(keyValue.getKey()).build();
            VectorWithId vectorWithId = VectorWithId.builder().id(longId).vector(vector).tableData(tableData).build();
            indexService(MAPPER.idTo(indexId), regionId).vectorAdd(
                requestTs,
                VectorAddRequest.builder().vectors(singletonList(vectorWithId)).build()
            );
        }

        private SearchNest getSearch(
            IndexType indexType, Map<String, Object> parameterMap
        ) {
            Object o;
            switch (indexType) {
                case VECTOR_DISKANN:
                    return SearchNest.Diskann.builder().build();
                case VECTOR_IVF_FLAT:
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
                case VECTOR_IVF_PQ:
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
                case VECTOR_HNSW:
                    int efSearch = 10;
                    o = parameterMap.get("efSearch");
                    if (o != null) {
                        efSearch = ((Number) o).intValue();
                    }

                    return SearchNest.Hnsw.builder().efSearch(efSearch).build();
                case VECTOR_FLAT:
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
