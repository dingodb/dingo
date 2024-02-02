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
import io.dingodb.client.operation.impl.DeleteRangeResult;
import io.dingodb.client.operation.impl.OpKeyRange;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.common.utils.Parameters;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class DingoClientV2 {

    private final String schema;

    private OperationServiceV2 operationService;

    public DingoClientV2(String coordinatorSvr) {
        this(coordinatorSvr, "DINGO");
    }

    public DingoClientV2(String coordinatorSvr, String schema) {
        operationService = new OperationServiceV2(coordinatorSvr);
        this.schema = schema.toUpperCase();
    }

    public DingoClientV2(String schema, OperationServiceV2 operationService) {
        this.schema = schema.toUpperCase();
        this.operationService = operationService;
    }

    public Table getTable(String schema, String tableName) {
        return operationService.getTable(schema, schema);
    }

    public boolean upsert(String tableName, Record record) {
        return Parameters.cleanNull(upsert(tableName, Collections.singletonList(record)).get(0), false);
    }

    public List<Boolean> upsert(String tableName, List<Record> records) {
        return upsert(schema, tableName, records);
    }

    public List<Boolean> upsert(String schema, String tableName, List<Record> records) {
        List<Object[]> tuples = records.stream().map(Record::getDingoColumnValuesInOrder).collect(Collectors.toList());
        return Arrays.stream(operationService.insert(schema, tableName, tuples)).collect(Collectors.toList());
    }

    /**
     * Insert table data and index(scalar + vector) data at the same time.
     * @param tableName table name
     * @param record record
     * @return is success
     */
    public boolean upsertIndex(String tableName, Object[] record) {
        return operationService.insert(schema, tableName, Collections.singletonList(record))[0];
    }

    public boolean putIfAbsent(String tableName, Record record) {
        return Parameters.cleanNull(putIfAbsent(tableName, Collections.singletonList(record)).get(0), false);
    }

    public List<Boolean> putIfAbsent(String tableName, List<Record> records) {
        List<Object[]> tuples = records.stream().map(Record::getDingoColumnValuesInOrder).collect(Collectors.toList());
        return Arrays.stream(operationService.insert(schema, tableName, tuples)).collect(Collectors.toList());
    }

    public boolean compareAndSet(String tableName, Record record, Record expect) {
        return Parameters.cleanNull(
            compareAndSet(tableName, Collections.singletonList(record), Collections.singletonList(expect)).get(0), false
        );
    }

    public List<Boolean> compareAndSet(String tableName, List<Record> records, List<Record> expects) {
        List<Object[]> old = records.stream().map(Record::getDingoColumnValuesInOrder).collect(Collectors.toList());
        List<Object[]> row = expects.stream().map(Record::getDingoColumnValuesInOrder).collect(Collectors.toList());
        return Arrays.stream(operationService.compareAndSet(schema, tableName, old, row))
            .collect(Collectors.toList());
    }

    /**
     * Update table data and index(scalar and vector) data at the same time.
     * @param record record
     * @param tableName  table name
     * @param expect expect
     * @return is success
     */
    public Boolean compareAndSetIndex(String tableName, Object[] record, Object[] expect) {
        return operationService.compareAndSet(
            schema, tableName, Collections.singletonList(record), Collections.singletonList(expect))[0];
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
        return operationService.get(schema, tableName, keys);
    }

    public Record get(final String tableName, final Key firstKey, List<String> colNames) {
        return Optional.mapOrNull(get(tableName, firstKey), r -> r.extract(colNames));
    }

    public Iterator<Record> scan(final String tableName, Key begin, Key end, boolean withBegin, boolean withEnd) {
        return operationService.scan(schema, tableName, new OpKeyRange(begin, end, withBegin, withEnd));
    }

    /*public Iterator<Record> scan(
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
    }*/

    public boolean delete(final String tableName, Key key) {
        return Parameters.cleanNull(delete(tableName, Collections.singletonList(key)).get(0), false);
    }

    public List<Boolean> delete(final String tableName, List<Key> keys) {
        return delete(schema, tableName, keys);
    }

    public List<Boolean> delete(String schema, final String tableName, List<Key> keys) {
        return Arrays.stream(operationService.delete(schema, tableName, keys)).collect(Collectors.toList());
    }

    public DeleteRangeResult delete(String tableName, Key begin, Key end, boolean withBegin, boolean withEnd) {
        return operationService.rangeDelete(schema, tableName, begin, end, withBegin, withEnd);
    }

    public boolean delete(String schema, String tableName, Key key) {
        return operationService.delete(schema, tableName, Collections.singletonList(key))[0];
    }

    public void close() {
        operationService.close();
    }
}
