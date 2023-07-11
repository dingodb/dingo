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
import io.dingodb.client.common.VectorWithId;
import io.dingodb.client.operation.impl.*;
import io.dingodb.common.util.Optional;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.index.Index;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.sdk.service.meta.AutoIncrementService;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class DingoClient {

    private final String schema;

    private OperationService operationService;
    private IndexService indexService;

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

    public boolean dropTable(String tableName) {
        return dropTable(schema, tableName);
    }

    public boolean dropTable(String schema, String table) {
        return operationService.dropTable(schema, table);
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

    public List<Record> getNotStandard(String tableName, List<Key> keys) {
        return operationService.exec(schema, tableName, GetOperation.getNotStandardInstance(), keys);
    }

    public Record get(final String tableName, final Key firstKey, List<String> colNames) {
        return Optional.mapOrNull(get(tableName, firstKey), r -> r.extract(colNames));
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

    public List<Boolean> deleteNotStandard(final String tableName, List<Key> keys) {
        return operationService.exec(schema, tableName, DeleteOperation.getNotStandardInstance(), keys);
    }

    public DeleteRangeResult delete(String tableName, Key begin, Key end, boolean withBegin, boolean withEnd) {
        return operationService.exec(
            schema,
            tableName,
            DeleteRangeOperation.getInstance(),
            new OpKeyRange(begin, end, withBegin, withEnd)
        );
    }

    public boolean createIndex(String indexName, Index index) {
        return createIndex(schema, indexName, index);
    }

    public boolean createIndex(String schema, String indexName, Index index) {
        return indexService.createIndex(schema, indexName, index);
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

    public List<VectorWithId> vectorAdd(String indexName, List<VectorWithId> vectors) {
        return vectorAdd(schema, indexName, vectors);
    }

    public List<VectorWithId> vectorAdd(String schema, String indexName, List<VectorWithId> vectors) {
        return indexService.exec(schema, indexName, VectorAddOperation.getInstance(), vectors, VectorContext.builder().build());
    }

    public List<VectorWithId> vectorAdd(String schema, String indexName, List<VectorWithId> vectors,
                                        Boolean replaceDeleted, Boolean isUpdate) {
        VectorContext context = VectorContext.builder().replaceDeleted(replaceDeleted).isUpdate(isUpdate).build();
        return indexService.exec(schema, indexName, VectorAddOperation.getInstance(), vectors, context);
    }

    public VectorDistanceArray vectorSearch(String indexName, VectorSearch vectorSearch) {
        return vectorSearch(schema, indexName, vectorSearch);
    }

    public VectorDistanceArray vectorSearch(String schema, String indexName, VectorSearch vectorSearch) {
        return indexService.exec(schema, indexName, VectorSearchOperation.getInstance(), vectorSearch, VectorContext.builder().build());
    }

    public List<VectorWithId> vectorBatchQuery(String schema, String indexName, List<Long> ids,
                                               boolean withoutVectorData,
                                               boolean withScalarData,
                                               List<String> selectedKeys) {
        VectorContext vectorContext = VectorContext.builder()
            .withoutVectorData(withoutVectorData)
            .withScalarData(withScalarData)
            .selectedKeys(selectedKeys)
            .build();
        return indexService.exec(schema, indexName, VectorBatchQueryOperation.getInstance(), ids, vectorContext);
    }

    /**
     *
     * @param schema schema name, default is 'dingo'
     * @param indexName index name
     * @param isGetMin if true, get min id, else get max id
     * @return id
     */
    public Long vectorGetBorderId(String schema, String indexName, Boolean isGetMin) {
        return indexService.exec(schema, indexName, VectorGetIdOperation.getInstance(), isGetMin, VectorContext.builder().build());
    }

    public List<Boolean> vectorDelete(String indexName, List<Long> ids) {
        return vectorDelete(schema, indexName, ids);
    }

    public List<Boolean> vectorDelete(String schema, String indexName, List<Long> ids) {
        return indexService.exec(schema, indexName, VectorDeleteOperation.getInstance(), ids, VectorContext.builder().build());
    }

    public void close() {
        operationService.close();
    }
}
