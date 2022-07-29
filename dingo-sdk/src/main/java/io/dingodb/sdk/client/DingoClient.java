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

package io.dingodb.sdk.client;

import io.dingodb.common.operation.ExecutiveResult;
import io.dingodb.common.operation.context.FilterContext;
import io.dingodb.common.operation.filter.Filter;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.operation.Column;
import io.dingodb.sdk.common.Key;
import io.dingodb.common.operation.Operation;
import io.dingodb.sdk.common.Record;
import io.dingodb.common.operation.Value;
import io.dingodb.sdk.operation.ContextForClient;
import io.dingodb.sdk.operation.ResultForClient;
import io.dingodb.sdk.operation.StoreOperationType;
import io.dingodb.sdk.operation.StoreOperationUtils;
import io.dingodb.sdk.utils.DingoClientException;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DingoClient {

    /**
     * Connection to Dingo Cluster.
     */
    private DingoConnection connection;

    /**
     * Operation Utils.
     */
    private StoreOperationUtils storeOpUtils;

    public static Integer retryTimes = 10;
    public static volatile boolean isConnectionInit = false;


    public DingoClient(String coordinatorExchangeSvrList) {
        this(coordinatorExchangeSvrList, retryTimes);
    }

    public DingoClient(String coordinatorExchangeSvrList, Integer retryTimes) {
        connection = new DingoConnection(coordinatorExchangeSvrList);
        this.retryTimes = retryTimes;
    }

    /**
     * connection must be init before do operation.
     * @return true or false
     */
    public boolean openConnection() {
        try {
            if (isConnected()) {
                return true;
            } else {
                connection.initConnection();
                storeOpUtils = new StoreOperationUtils(connection, retryTimes);
                isConnectionInit = true;
            }
            return true;
        } catch (Exception e) {
            log.error("init connection failed", e.toString(), e);
            return false;
        }
    }

    public boolean isConnected() {
        return isConnectionInit;
    }

    public void closeConnection() {
        // todo Huzx
        if (storeOpUtils != null) {
            storeOpUtils.clearTableDefinitionInCache();
        }
        isConnectionInit = false;
    }


    public boolean createTable(TableDefinition tableDef) {
        if (!isConnected()) {
            log.error("connection has not been initialized, please call openConnection first");
            return false;
        }

        if (tableDef == null || tableDef.getName() == null || tableDef.getName().isEmpty()) {
            log.error("Invalid TableDefinition:{}", tableDef);
            return false;
        }

        /**
         * define table definition and call create table api.
         */
        boolean isSuccess = false;
        try {
            connection.getMetaClient().createTable(tableDef.getName(), tableDef);
            isSuccess = true;
            storeOpUtils.updateCacheOfTableDefinition(tableDef.getName(), tableDef);
        } catch (Exception e) {
            isSuccess = false;
            log.error("create table: {} definition:{} failed:{}",
                tableDef.getName(),
                tableDef,
                e.toString(), e);
        }
        return isSuccess;
    }

    public boolean dropTable(final String tableName) {
        if (!isConnected()) {
            log.error("connection has not been initialized, please call openConnection first");
            return false;
        }
        if (tableName == null || tableName.isEmpty()) {
            log.error("Invalid table name:{}", tableName);
            return false;
        }
        boolean isSuccess = false;
        try {
            isSuccess = connection.getMetaClient().dropTable(tableName.toUpperCase());
        } catch (Exception e) {
            isSuccess = false;
            log.error("drop table: {} failed:{}", tableName, e.toString(), e);
        } finally {
            storeOpUtils.removeCacheOfTableDefinition(tableName);
        }
        return isSuccess;
    }

    public TableDefinition getTableDefinition(final String tableName) {
        if (!isConnected()) {
            log.error("connection has not been initialized, please call openConnection first");
            return null;
        }
        if (tableName == null || tableName.isEmpty()) {
            log.error("Invalid table name:{}", tableName);
            return null;
        }
        TableDefinition tableDef = storeOpUtils.getTableDefinition(tableName);
        return tableDef;
    }

    public boolean insert(String tableName, List<Object[]> records) {
        if (!isConnected()) {
            log.error("connection has not been initialized, please call openConnection first");
            return false;
        }
        if (records == null || records.size() == 0) {
            log.error("Invalid input rowList{}", records);
            return false;
        }

        HashMap<Key, Record> resultList = convertObjectArray2Record(tableName, records);
        List<Key> keyList = new ArrayList<>();
        List<Record> recordList = new ArrayList<>();
        for (Map.Entry<Key, Record> recordEntry: resultList.entrySet()) {
            keyList.add(recordEntry.getKey());
            recordList.add(recordEntry.getValue());
        }
        return doPut(keyList, recordList);
    }

    public boolean insert(String tableName, Object[] row) {
        List<Object[]> inputRecords = new ArrayList<>();
        inputRecords.add(row);
        return insert(tableName, inputRecords);
    }

    private HashMap<Key, Record> convertObjectArray2Record(String tableName, List<Object[]> recordList) {
        TableDefinition tableDefinition = storeOpUtils.getTableDefinition(tableName);
        if (tableDefinition == null) {
            log.warn("table:{} definition not found", tableName);
            return null;
        }
        HashMap<Key, Record>  recordResults = new HashMap<>(recordList.size());
        for (Object[] record: recordList) {
            if (record == null || record.length == 0
                || tableDefinition.getColumnsCount() != record.length) {
                log.error("Invalid record:{}, count: expect:{}, real:{}",
                    record, tableDefinition.getColumnsCount(), record != null ? record.length : 0);
                return null;
            }
            List<Value> userKeys = new ArrayList<>();
            List<String> columnNames = new ArrayList<>();

            int index = 0;
            for (ColumnDefinition column : tableDefinition.getColumns()) {
                if (column.isPrimary()) {
                    userKeys.add(Value.get(record[index]));
                }
                columnNames.add(column.getName());
                index++;
            }

            int columnCnt = tableDefinition.getColumnsCount();
            Column[] columns = new Column[columnCnt];
            for (int i = 0; i < columnCnt; i++) {
                columns[i] = new Column(columnNames.get(i), Value.get(record[i]));
            }
            Key key = new Key("DINGO", tableName, userKeys);
            Record record1 = new Record(tableDefinition.getColumns(), columns);
            recordResults.put(key, record1);
        }
        return recordResults;
    }

    public boolean put(Key key, Column[] columns) throws DingoClientException {
        TableDefinition tableDefinition = storeOpUtils.getTableDefinition(key.getTable());
        Record record = new Record(tableDefinition.getColumns(), columns);
        return doPut(Arrays.asList(key), Arrays.asList(record));
    }

    public boolean put(List<Key> keyList, List<Record> recordList) throws DingoClientException {
        return doPut(keyList, recordList);
    }

    public Object[] get(String tableName, Object[] key) {
        return null;
    }

    public Record get(Key key) throws Exception {
        List<Record> records = doGet(Arrays.asList(key));
        if (records == null || records.isEmpty()) {
            return null;
        }
        return records.get(0);
    }

    public List<Record> get(List<Key> keyList) throws Exception {
        return doGet(keyList);
    }

    public boolean delete(String tableName, Object[] key) {
        return true;
    }

    public boolean delete(Key key) throws Exception {
        return doDelete(Arrays.asList(key));
    }

    public boolean delete(List<Key> keyList) throws Exception {
        return doDelete(keyList);
    }

    private List<Record> doGet(List<Key> keyList) throws Exception {
        ResultForClient result = storeOpUtils.doOperation(
            StoreOperationType.GET,
            keyList.get(0).getTable(),
            new ContextForClient(keyList, Collections.emptyList(), null, null));
        if (!result.getStatus()) {
            log.error("Execute get command failed:{}", result.getErrorMessage());
            return null;
        } else {
            return result.getRecords();
        }
    }

    private boolean doPut(List<Key> keyList, List<Record> recordList) {
        ResultForClient result = storeOpUtils.doOperation(
            StoreOperationType.PUT,
            keyList.get(0).getTable(),
            new ContextForClient(keyList, Collections.emptyList(), recordList, null));
        if (!result.getStatus()) {
            log.error("Execute put command failed:{}", result.getErrorMessage());
            return false;
        }
        return true;
    }

    private boolean doDelete(List<Key> keyList) {
        ResultForClient result = storeOpUtils.doOperation(
            StoreOperationType.DELETE,
            keyList.get(0).getTable(),
            new ContextForClient(keyList, Collections.emptyList(),null, null));
        if (!result.getStatus()) {
            log.error("Execute put command failed:{}", result.getErrorMessage());
            return false;
        }
        return true;
    }

    public final List<ExecutiveResult> add(@Nonnull Key start, @Nonnull Column... columns) {
        return add(start, null, columns);
    }

    public final List<ExecutiveResult> add(@Nonnull Key start, Key end, @Nonnull Column... columns) {
        if (end != null && start.userKey.size() != end.userKey.size()) {
            log.error("The number of primary keys in the start:{} and end:{} ranges is different", start, end);
            return Arrays.asList(new ExecutiveResult(null, false));
        }
        Operation operation = Operation.add(columns);
        ContextForClient contextForClient = new ContextForClient(
            Arrays.asList(start),
            end == null ? null : Arrays.asList(end),
            null,
            Arrays.asList(operation)
        );
        return storeOpUtils.doOperation(start.getTable(), contextForClient);
    }

    public final List<ExecutiveResult> max(@Nonnull Key start, @Nonnull Column... columns) {
        return max(start, null, columns);
    }

    public final List<ExecutiveResult> max(@Nonnull Key start, Key end, @Nonnull Column... columns) {
        if (end != null && start.userKey.size() != end.userKey.size()) {
            log.error("The number of primary keys in the start:{} and end:{} ranges is different", start, end);
            return null;
        }
        Operation operation = Operation.max(columns);
        ContextForClient contextForClient = new ContextForClient(
            Arrays.asList(start),
            end == null ? null : Arrays.asList(end),
            null,
            Arrays.asList(operation)
        );
        return storeOpUtils.doOperation(start.getTable(), contextForClient);
    }

    public final List<ExecutiveResult> min(@Nonnull Key start, @Nonnull Column... columns) {
        return min(start, null, columns);
    }

    public final List<ExecutiveResult> min(@Nonnull Key start, Key end, @Nonnull Column... columns) {
        if (end != null && start.userKey.size() != end.userKey.size()) {
            log.error("The number of primary keys in the start:{} and end:{} ranges is different", start, end);
            return null;
        }
        Operation operation = Operation.min(columns);
        ContextForClient contextForClient = new ContextForClient(
            Arrays.asList(start),
            end == null ? null : Arrays.asList(end),
            null,
            Arrays.asList(operation)
        );
        return storeOpUtils.doOperation(start.getTable(), contextForClient);
    }

    public final List<ExecutiveResult> sum(@Nonnull Key start, @Nonnull Column... columns) {
        return sum(start, null, null, columns);
    }

    // todo modify filter
    public final List<ExecutiveResult> sum(@Nonnull Key start, Key end, FilterContext context, @Nonnull Column... columns) {
        if (end != null && start.userKey.size() != end.userKey.size()) {
            log.error("The number of primary keys in the start:{} and end:{} ranges is different", start, end);
            return null;
        }
        Operation operation = Operation.sum(columns);
        operation.operationContext.filterContext(context);
        ContextForClient contextForClient = new ContextForClient(
            Arrays.asList(start),
            end == null ? null : Arrays.asList(end),
            null,
            Arrays.asList(operation)
        );
        return storeOpUtils.doOperation(start.getTable().toUpperCase(), contextForClient);
    }

    public final List<ExecutiveResult> count(@Nonnull Key start, Key end, @Nonnull Column... columns) {
        if (end != null && start.userKey.size() != end.userKey.size()) {
            log.error("The number of primary keys in the start:{} and end:{} ranges is different", start, end);
            return null;
        }
        Operation operation = Operation.count(columns);
        ContextForClient contextForClient = new ContextForClient(
            Arrays.asList(start),
            Arrays.asList(end),
            null,
            Arrays.asList(operation)
        );

        return storeOpUtils.doOperation(start.getTable().toUpperCase(), contextForClient);
    }

    public final List<ExecutiveResult> append(@Nonnull Key key, @Nonnull Column... columns) {
        Operation operation = Operation.append(columns);
        ContextForClient contextForClient = new ContextForClient(
            Arrays.asList(key),
            Collections.emptyList(),
            null,
            Arrays.asList(operation)
        );

        return storeOpUtils.doOperation(key.getTable().toUpperCase(), contextForClient);
    }

    public final List<ExecutiveResult> replace(@Nonnull Key key, @Nonnull Column... columns) {
        Operation operation = Operation.replace(columns);
        ContextForClient contextForClient = new ContextForClient(
            Arrays.asList(key),
            Collections.emptyList(),
            null,
            Arrays.asList(operation)
        );
        return storeOpUtils.doOperation(key.getTable().toUpperCase(), contextForClient);
    }

    public final List<ExecutiveResult> operate(Key start, List<Operation> operations) {
        return operate(start, null, operations);
    }

    /**
     * Perform multiple read/write operations on a single key in one batch call.
     *<p>
     *     Operation and ListOperation, MapOperation can be performed in same call.
     *</p>
     *
     * @param start unique record identifier
     * @param operations database operations to perform
     * @return .
     */
    public final List<ExecutiveResult> operate(Key start, Key end, List<Operation> operations) {
        ContextForClient contextForClient = new ContextForClient(
            Arrays.asList(start),
            end == null ? null : Arrays.asList(end),
            null,
            operations);
        return storeOpUtils.doOperation(start.getTable().toUpperCase(), contextForClient);
    }
}
