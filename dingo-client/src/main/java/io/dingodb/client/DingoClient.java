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
import io.dingodb.common.util.Optional;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.utils.Parameters;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class DingoClient {

    private final String schema;

    private OperationService operationService;
    private IndexOperationService indexOperationService;

    public static Integer retryTimes = 20;

    public DingoClient(String coordinatorSvr) {
        this(coordinatorSvr, retryTimes);
    }

    public DingoClient(String coordinatorSvr, Integer retryTimes) {
        this(coordinatorSvr, "DINGO", retryTimes);
    }

    public DingoClient(String coordinatorSvr, String schema, Integer retryTimes) {
        operationService = new OperationService(coordinatorSvr, retryTimes);
        indexOperationService = new IndexOperationService(coordinatorSvr, retryTimes);
        this.schema = schema.toUpperCase();
    }

    public DingoClient(String schema, OperationService operationService) {
        this.schema = schema.toUpperCase();
        this.operationService = operationService;
    }

    public boolean open() {
        operationService.init();
        return true;
    }

    @Deprecated
    public boolean createTable(Table table) {
        return createTable(schema, table);
    }

    @Deprecated
    public boolean createTable(String schema, Table table) {
        return operationService.createTable(schema, table.getName(), table);
    }

    @Deprecated
    public boolean dropTable(String tableName) {
        return dropTable(schema, tableName);
    }

    @Deprecated
    public boolean dropTable(String schema, String table) {
        return operationService.dropTable(schema, table);
    }

    @Deprecated
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

    public void close() {
        operationService.close();
    }
}
