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

package io.dingodb.client;

import io.dingodb.client.common.Key;
import io.dingodb.client.common.Record;
import io.dingodb.client.common.VectorDistanceArray;
import io.dingodb.client.common.VectorSearch;
import io.dingodb.client.common.VectorWithDistance;
import io.dingodb.client.common.VectorWithId;
import io.dingodb.client.operation.impl.CompareAndSetOperation;
import io.dingodb.client.operation.impl.DeleteOperation;
import io.dingodb.client.operation.impl.DeleteRangeOperation;
import io.dingodb.client.operation.impl.DeleteRangeResult;
import io.dingodb.client.operation.impl.GetOperation;
import io.dingodb.client.operation.impl.KeyRangeCoprocessor;
import io.dingodb.client.operation.impl.OpKeyRange;
import io.dingodb.client.operation.impl.Operation;
import io.dingodb.client.operation.impl.PutIfAbsentOperation;
import io.dingodb.client.operation.impl.PutOperation;
import io.dingodb.client.operation.impl.ScanCoprocessorOperation;
import io.dingodb.client.operation.impl.ScanOperation;
import io.dingodb.client.operation.impl.VectorAddOperation;
import io.dingodb.client.operation.impl.VectorBatchQueryOperation;
import io.dingodb.client.operation.impl.VectorCalcDistanceOperation;
import io.dingodb.client.operation.impl.VectorCountOperation;
import io.dingodb.client.operation.impl.VectorDeleteOperation;
import io.dingodb.client.operation.impl.VectorGetIdOperation;
import io.dingodb.client.operation.impl.VectorGetRegionMetricsOperation;
import io.dingodb.client.operation.impl.VectorScanQueryOperation;
import io.dingodb.client.operation.impl.VectorSearchOperation;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.util.Optional;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.index.Index;
import io.dingodb.sdk.common.index.IndexMetrics;
import io.dingodb.sdk.common.index.VectorIndexParameter;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.sdk.common.vector.Vector;
import io.dingodb.sdk.common.vector.VectorCalcDistance;
import io.dingodb.sdk.common.vector.VectorDistanceRes;
import io.dingodb.sdk.common.vector.VectorIndexMetrics;
import io.dingodb.sdk.common.vector.VectorScanQuery;
import io.dingodb.sdk.service.meta.AutoIncrementService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class DingoClient {

    private final String schema;

    private OperationService operationService;
    private IndexOperationService indexOperationService;
    private IndexService indexService;

    public static final int MAX_MESSAGE_SIZE = 8 * 1024 * 1024;

    public static Integer retryTimes = 20;

    public DingoClient(String coordinatorSvr) {
        this(coordinatorSvr, retryTimes);
    }

    public DingoClient(String coordinatorSvr, Integer retryTimes) {
        this(coordinatorSvr, "dingo", retryTimes);
    }

    public DingoClient(String coordinatorSvr, String schema, Integer retryTimes) {
        operationService = new OperationService(coordinatorSvr, retryTimes);
        indexService = new IndexService(coordinatorSvr, new AutoIncrementService(coordinatorSvr), retryTimes);
        indexOperationService = new IndexOperationService(coordinatorSvr, retryTimes);
        this.schema = schema;
    }

    public DingoClient(String schema, OperationService operationService) {
        this.schema = schema;
        this.operationService = operationService;
    }

    public boolean open() {
        operationService.init();
        return true;
    }

    public boolean createTable(Table table) {
        return createTable(schema, table);
    }

    public boolean createTable(String schema, Table table) {
        return operationService.createTable(schema, table.getName(), table);
    }

    public boolean createTables(String schema, Table table, List<Table> indexes) {
        return operationService.createTables(schema, table, indexes);
    }

    public boolean dropTable(String tableName) {
        return dropTable(schema, tableName);
    }

    public boolean dropTable(String schema, String table) {
        return operationService.dropTable(schema, table);
    }

    public boolean dropTables(String schema, List<String> tables) {
        return operationService.dropTables(schema, tables);
    }

    public Table getTableDefinition(final String tableName) {
        return getTableDefinition(schema, tableName);
    }

    public Table getTableDefinition(String schema, String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new DingoClientException("Invalid table name: " + tableName);
        }
        return operationService.getTableDefinition(schema, tableName);
    }

    public List<Table> getTables(String schema, String tableName) {
        return operationService.getTables(schema, tableName);
    }

    public Any exec(String tableName, Operation operation, Any parameter) {
        return operationService.exec(schema, tableName, operation, parameter);
    }

    public boolean upsert(String tableName, Record record) {
        return Parameters.cleanNull(upsert(tableName, Collections.singletonList(record)).get(0), false);
    }

    public List<Boolean> upsert(String tableName, List<Record> records) {
        return upsert(schema, tableName, records);
    }

    public List<Boolean> upsert(String schema, String tableName, List<Record> records) {
        return operationService.exec(schema, tableName, PutOperation.getInstance(), records);
    }

    /**
     * Insert table data and index(scalar + vector) data at the same time.
     * @param tableName table name
     * @param record record
     * @return is success
     */
    public boolean upsertIndex(String tableName, Object[] record) {
        return indexOperationService.exec(
            schema, tableName,
            PutOperation.getInstance(),
            new IndexOperationService.Parameter(record));
    }

    public List<Boolean> upsertNotStandard(String tableName, List<Record> records) {
        return operationService.exec(schema, tableName, PutOperation.getNotStandardInstance(), records);
    }

    public boolean putIfAbsent(String tableName, Record record) {
        return Parameters.cleanNull(putIfAbsent(tableName, Collections.singletonList(record)).get(0), false);
    }

    public List<Boolean> putIfAbsent(String tableName, List<Record> records) {
        return operationService.exec(schema, tableName, PutIfAbsentOperation.getInstance(), records);
    }

    public List<Boolean> putIfAbsentNotStandard(String tableName, List<Record> records) {
        return operationService.exec(schema, tableName, PutIfAbsentOperation.getNotStandardInstance(), records);
    }

    public boolean compareAndSet(String tableName, Record record, Record expect) {
        return Parameters.cleanNull(
            compareAndSet(tableName, Collections.singletonList(record), Collections.singletonList(expect)).get(0), false
        );
    }

    public List<Boolean> compareAndSet(String tableName, List<Record> records, List<Record> expects) {
        return operationService.exec(
            schema,
            tableName,
            CompareAndSetOperation.getInstance(),
            new CompareAndSetOperation.Parameter(records, expects)
        );
    }

    /**
     * Update table data and index(scalar and vector) data at the same time.
     * @param record record
     * @param tableName  table name
     * @param expect expect
     * @return is success
     */
    public Boolean compareAndSetIndex(String tableName, Object[] record, Object[] expect) {
        return indexOperationService.exec(
            schema,
            tableName,
            CompareAndSetOperation.getInstance(),
            new IndexOperationService.Parameter(record, expect));
    }

    public List<Boolean> compareAndSetNotStandard(String tableName, List<Record> records, List<Record> expects) {
        return operationService.exec(
            schema,
            tableName,
            CompareAndSetOperation.getNotStandardInstance(),
            new CompareAndSetOperation.Parameter(records, expects)
        );
    }

    public Record get(String tableName, Key key) {
        List<Record> records = get(tableName, Collections.singletonList(key));
        if (records != null && records.size() > 0) {
            return records.get(0);
        }
        return null;
    }

    public List<Record> get(String tableName, List<Key> keys) {
        return get(schema, tableName, keys);
    }

    public List<Record> get(String schema, String tableName, List<Key> keys) {
        return operationService.exec(schema, tableName, GetOperation.getInstance(), keys);
    }

    public Record get(final String tableName, final Key firstKey, List<String> colNames) {
        return Optional.mapOrNull(get(tableName, firstKey), r -> r.extract(colNames));
    }

    public List<Record> getNotStandard(String tableName, List<Key> keys) {
        return operationService.exec(schema, tableName, GetOperation.getNotStandardInstance(), keys);
    }

    public Iterator<Record> scan(final String tableName, Key begin, Key end, boolean withBegin, boolean withEnd) {
        return operationService.exec(
            schema, tableName, ScanOperation.getInstance(), new OpKeyRange(begin, end, withBegin, withEnd)
        );
    }

    public Iterator<Record> scan(
        final String tableName, Key begin, Key end, boolean withBegin, boolean withEnd,
        List<KeyRangeCoprocessor.Aggregation> aggregationOperators) {
        return scan(tableName, begin, end, withBegin, withEnd, aggregationOperators, Collections.emptyList());
    }

    public Iterator<Record> scan(
        final String tableName, Key begin, Key end, boolean withBegin, boolean withEnd,
        List<KeyRangeCoprocessor.Aggregation> aggregationOperators,
        List<String> groupBy) {
        return operationService.exec(
            schema,
            tableName,
            ScanCoprocessorOperation.getInstance(),
            new KeyRangeCoprocessor(new OpKeyRange(begin, end, withBegin, withEnd), aggregationOperators, groupBy)
        );
    }

    public Iterator<Record> scanNotStandard(
        final String tableName, Key begin, Key end, boolean withBegin, boolean withEnd,
        List<KeyRangeCoprocessor.Aggregation> aggregationOperators,
        List<String> groupBy) {
        return operationService.exec(
            schema,
            tableName,
            ScanCoprocessorOperation.getNotStandardInstance(),
            new KeyRangeCoprocessor(new OpKeyRange(begin, end, withBegin, withEnd), aggregationOperators, groupBy)
        );
    }

    public boolean delete(final String tableName, Key key) {
        return Parameters.cleanNull(delete(tableName, Collections.singletonList(key)).get(0), false);
    }

    public List<Boolean> delete(final String tableName, List<Key> keys) {
        return delete(schema, tableName, keys);
    }

    public List<Boolean> delete(String schema, final String tableName, List<Key> keys) {
        return operationService.exec(schema, tableName, DeleteOperation.getInstance(), keys);
    }

    public DeleteRangeResult delete(String tableName, Key begin, Key end, boolean withBegin, boolean withEnd) {
        return operationService.exec(
            schema,
            tableName,
            DeleteRangeOperation.getInstance(),
            new OpKeyRange(begin, end, withBegin, withEnd)
        );
    }

    /**
     * Delete table data and index(scalar + vector) data at the same time.
     * @param tableName table name
     * @param key key
     * @param schema schema
     * @return is success
     */
    public boolean delete(String schema, String tableName, Key key) {
        return indexOperationService.exec(
            schema,
            tableName,
            DeleteOperation.getInstance(),
            new IndexOperationService.Parameter(get(tableName, key).getDingoColumnValuesInOrder()));
    }

    public List<Boolean> deleteNotStandard(final String tableName, List<Key> keys) {
        return operationService.exec(schema, tableName, DeleteOperation.getNotStandardInstance(), keys);
    }

    public boolean createIndex(String indexName, Index index) {
        return createIndex(schema, indexName, index);
    }

    public boolean createIndex(String schema, String indexName, Index index) {
        return indexService.createIndex(schema, indexName, index);
    }

    public boolean updateIndex(String index, Index newIndex) {
        return indexService.updateIndex(schema, index, newIndex);
    }

    public boolean updateIndex(String schema, String index, Index newIndex) {
        return indexService.updateIndex(schema, index, newIndex);
    }

    public boolean dropIndex(String indexName) {
        return dropIndex(schema, indexName);
    }

    public boolean dropIndex(String schema, String indexName) {
        return indexService.dropIndex(schema, indexName);
    }

    public Index getIndex(String index) {
        return getIndex(schema, index);
    }

    public Index getIndex(String schema, String index) {
        return indexService.getIndex(schema, index);
    }

    public List<Index> getIndexes(String schema) {
        return indexService.getIndexes(schema);
    }

    public IndexMetrics getIndexMetrics(String schema, String index) {
        return indexService.getIndexMetrics(schema, index);
    }

    public List<VectorWithId> vectorAdd(String indexName, List<VectorWithId> vectors) {
        return vectorAdd(schema, indexName, vectors);
    }

    public List<VectorWithId> vectorAdd(String schema, String indexName, List<VectorWithId> vectors) {
        return vectorAdd(schema, indexName, vectors, false, false);
    }

    public List<VectorWithId> vectorAdd(String schema, String indexName, List<VectorWithId> vectors,
                                        Boolean replaceDeleted, Boolean isUpdate) {
        VectorContext context = VectorContext.builder().replaceDeleted(replaceDeleted).isUpdate(isUpdate).build();
        int dimension = getDimension(schema, indexName);
        long count = checkDimension(vectors, dimension);
        int messageSize = getMessageSize(vectors);
        if (vectors.size() >= 1024) {
            throw new DingoClientException("Param vectors size " + vectors.size() + " is exceed max batch count 1024");
        }
        if (messageSize > MAX_MESSAGE_SIZE) {
            throw new DingoClientException("Message exceeds maximum size " + MAX_MESSAGE_SIZE + " : " + messageSize);
        }
        if (dimension != 0 && count > 0) {
            throw new DingoClientException("Dimension is not the same length as its value or from the time it was created");
        }
        return indexService.exec(schema, indexName, VectorAddOperation.getInstance(), vectors, context);
    }

    /**
     * Get the total size of the vector.
     *
     * @param vectors vectors
     * @return Total message size
     */
    private static int getMessageSize(List<VectorWithId> vectors) {
        int totalSize;
        if (vectors.get(0).getVector().getValueType().equals(Vector.ValueType.BINARY)) {
            totalSize = vectors.stream()
                .map(v -> v.getVector().getBinaryValues())
                .map(l -> l.stream()
                    .map(b -> b.length)
                    .reduce(Integer::sum)
                    .orElse(0))
                .reduce(Integer::sum)
                .orElse(0);
        } else {
            totalSize = ProtostuffCodec.write(vectors).length;
        }
        return totalSize;

    }

    private static long checkDimension(List<VectorWithId> vectors, int dimension) {
        return vectors.stream()
            .map(VectorWithId::getVector)
            .filter(v -> v.getDimension() != dimension || (v.getValueType() == Vector.ValueType.FLOAT
                    ? v.getFloatValues().size() != dimension : v.getBinaryValues().size() != dimension))
            .count();
    }

    private int getDimension(String schema, String indexName) {
        Index index = indexService.getIndex(schema, indexName, false);
        VectorIndexParameter parameter = index.getIndexParameter().getVectorIndexParameter();
        int dimension;
        switch (parameter.getVectorIndexType()) {
            case VECTOR_INDEX_TYPE_FLAT:
                dimension = parameter.getFlatParam().getDimension();
                break;
            case VECTOR_INDEX_TYPE_IVF_FLAT:
                dimension = parameter.getIvfFlatParam().getDimension();
                break;
            case VECTOR_INDEX_TYPE_IVF_PQ:
                dimension = parameter.getIvfPqParam().getDimension();
                break;
            case VECTOR_INDEX_TYPE_HNSW:
                dimension = parameter.getHnswParam().getDimension();
                break;
            case VECTOR_INDEX_TYPE_DISKANN:
                dimension = parameter.getDiskAnnParam().getDimension();
                break;
            default:
                dimension = 0;
        }
        return dimension;
    }

    public List<VectorDistanceArray> vectorSearch(String indexName, VectorSearch vectorSearch) {
        return vectorSearch(schema, indexName, vectorSearch);
    }

    public List<VectorDistanceArray> vectorSearch(String schema, String indexName, VectorSearch vectorSearch) {
        List<VectorDistanceArray> distanceArrays = indexService.exec(
            schema,
            indexName,
            VectorSearchOperation.getInstance(),
            vectorSearch);

        List<VectorDistanceArray> result = new ArrayList<>();
        for (VectorDistanceArray distanceArray : distanceArrays) {
            List<VectorWithDistance> withDistances = distanceArray.getVectorWithDistances();
            Integer topN = vectorSearch.getParameter().getTopN();
            if (withDistances.size() <= topN) {
                result.add(distanceArray);
                continue;
            }
            result.add(new VectorDistanceArray(withDistances.subList(0, topN)));
        }
        return result;
    }

    public Map<Long, VectorWithId> vectorBatchQuery(String schema, String indexName, Set<Long> ids,
                                              boolean withoutVectorData,
                                              boolean withoutScalarData,
                                              List<String> selectedKeys) {
        VectorContext vectorContext = VectorContext.builder()
            .withoutVectorData(withoutVectorData)
            .withoutScalarData(withoutScalarData)
            .selectedKeys(selectedKeys)
            .build();
        return indexService.exec(schema, indexName, VectorBatchQueryOperation.getInstance(), ids, vectorContext);
    }

    /**
     * Get Boundary by schema and index name.
     * @param schema schema name, default is 'dingo'
     * @param indexName index name
     * @param isGetMin if true, get min id, else get max id
     * @return id
     */
    public Long vectorGetBorderId(String schema, String indexName, Boolean isGetMin) {
        long[] longArr = indexService.exec(schema, indexName, VectorGetIdOperation.getInstance(), isGetMin);
        return (isGetMin ? Arrays.stream(longArr).min() : Arrays.stream(longArr).max()).getAsLong();
    }

    public List<VectorWithId> vectorScanQuery(String schema, String indexName, VectorScanQuery query) {
        List<VectorWithId> result = indexService.exec(schema, indexName, VectorScanQueryOperation.getInstance(), query);
        return query.getMaxScanCount() > result.size()
            ? result : result.subList(0, Math.toIntExact(query.getMaxScanCount()));
    }

    public VectorIndexMetrics getRegionMetrics(String schema, String indexName) {
        return indexService.exec(schema, indexName, VectorGetRegionMetricsOperation.getInstance(), null);
    }

    public List<Boolean> vectorDelete(String indexName, List<Long> ids) {
        return vectorDelete(schema, indexName, ids);
    }

    public List<Boolean> vectorDelete(String schema, String indexName, List<Long> ids) {
        return indexService.exec(schema, indexName, VectorDeleteOperation.getInstance(), ids);
    }

    public Long vectorCount(String schema, String indexName) {
        return indexService.exec(schema, indexName, VectorCountOperation.getInstance(), null);
    }

    public void close() {
        operationService.close();
    }
}
