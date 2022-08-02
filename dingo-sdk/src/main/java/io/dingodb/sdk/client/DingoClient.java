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

import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.sdk.common.Column;
import io.dingodb.sdk.common.Key;
import io.dingodb.sdk.common.Operation;
import io.dingodb.sdk.common.Record;
import io.dingodb.sdk.common.Value;
import io.dingodb.sdk.operation.ContextForClient;
import io.dingodb.sdk.operation.ResultForClient;
import io.dingodb.sdk.operation.StoreOperationType;
import io.dingodb.sdk.operation.StoreOperationUtils;
import io.dingodb.sdk.utils.DingoClientException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Instantiate an <code>DingoClient</code> object to access a dingo
 * database cluster and perform database operations.
 *
 * <p>Your application uses this class API to perform database operations such as
 * create table, drop table,
 * compute about numerical(min, max, add, sum),
 * key-value operation(writing, reading, update records),
 * operation on complex object(Array, Set, List, Map),
 * filter and aggeragation operation on selecting sets of records.
 *
 * <p>Each record may have multiple columns, you can operate on one record
 * or multiple records with a range of keys.
 */
@Slf4j
public class DingoClient {

    private DingoConnection connection;

    private StoreOperationUtils storeOpUtils;

    public static Integer retryTimes = 100;
    public static volatile boolean isConnectionInit = false;


    /**
     * construct instance of dingo client.
     * @param coordinatorExchangeSvrList coordinator exchange server list
     *                                   format: ip:port,ip:port,ip:port
     *                                   example: localhost:8080,localhost:8081,localhost:8082
     */
    public DingoClient(String coordinatorExchangeSvrList) {
        this(coordinatorExchangeSvrList, retryTimes);
    }

    /**
     * construct instance of dingo client with retry times.
     * @param coordinatorExchangeSvrList coordinator exchange server list
     *                                   format: ip:port,ip:port,ip:port
     *                                   example: localhost:8080,localhost:8081,localhost:8082
     * @param retryTimes this times is used to retry when operation is failed.
     */
    public DingoClient(String coordinatorExchangeSvrList, Integer retryTimes) {
        connection = new DingoConnection(coordinatorExchangeSvrList);
        this.retryTimes = retryTimes;
    }

    /**
     * build connection to dingo database cluster.
     * @return true when connection is built successfully, otherwise false.
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
        } catch (Exception ex) {
            log.error("init connection failed", ex);
            return false;
        }
    }

    /**
     * check connection is connected or not.
     * @return true when connection is connected, otherwise false.
     */
    public boolean isConnected() {
        return isConnectionInit;
    }

    /**
     * close connection to dingo database cluster.
     */
    public void closeConnection() {
        if (storeOpUtils != null) {
            storeOpUtils.shutdown();
        }
        isConnectionInit = false;
    }


    /**
     * create table using table definition.
     * @param tableDef definition of input table
     * @return true when table is created successfully, otherwise false.
     */
    public boolean createTable(TableDefinition tableDef) {
        if (!isConnected()) {
            log.error("connection has not been initialized, please call openConnection first");
            return false;
        }

        if (tableDef == null || tableDef.getName() == null || tableDef.getName().isEmpty()) {
            log.error("Invalid TableDefinition:{}", tableDef);
            return false;
        }

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

    /**
     * drop table using table name.
     * @param tableName input name of table
     * @return true when table is dropped successfully, otherwise false.
     */
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

    /**
     * get definition from dingo meta.
     * @param tableName input table name
     * @return TableDefinition about table, null if table not exist.
     */
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

    /**
     * insert records into table.
     * @param tableName input table name
     * @param records record(multiple records ordered by column index on table definition).
     * @return true when records are inserted successfully, otherwise false.
     */
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

    /**
     * insert records into table.
     * @param tableName input table name
     * @param record ordered by column index on table definition.
     * @return true when records are inserted successfully, otherwise false.
     */
    public boolean insert(String tableName, Object[] record) {
        List<Object[]> inputRecords = new ArrayList<>();
        inputRecords.add(record);
        return insert(tableName, inputRecords);
    }

    private HashMap<Key, Record> convertObjectArray2Record(String tableName,
                                                           List<Object[]> recordList) {
        TableDefinition tableDefinition = storeOpUtils.getTableDefinition(tableName);
        if (tableDefinition == null) {
            log.warn("table:{} definition not found", tableName);
            return null;
        }
        HashMap<Key, Record>  recordResults = new LinkedHashMap<>(recordList.size());
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

    /**
     * put record to table using key.
     * @param key input key of the table
     * @param columns input columns of the table(ordered by column index on table definition).
     * @return true when record is put successfully, otherwise false.
     */
    public boolean put(Key key, Column[] columns) {
        TableDefinition tableDefinition = storeOpUtils.getTableDefinition(key.getTable());
        Record record = new Record(tableDefinition.getColumns(), columns);
        return doPut(Arrays.asList(key), Arrays.asList(record));
    }

    /**
     * Note: The length of <strong>keyList</strong> and <strong>recordList</strong>MUST be equal!!!
     * put multiple records together into table.
     * @param keyList input key list of the table
     * @param recordList input value list of the table(ordered by column index on table definition).
     * @return true when records are put successfully, otherwise false.
     *
     */
    public boolean put(List<Key> keyList, List<Record> recordList) {
        return doPut(keyList, recordList);
    }

    /**
     * get table record by input key.
     * @param tableName input table name
     * @param key input key of the table
     * @return record of the table in array of object, null if not found.
     */
    public Object[] get(String tableName, Object[] key) {
        List<Value> userKeys = new ArrayList<>();
        for (Object keyValue: key) {
            userKeys.add(Value.get(keyValue));
        }
        Key dingoKey = new Key("DINGO", tableName, userKeys);
        Record dingoRecord = get(dingoKey);
        return dingoRecord.getDingoColumnValuesInOrder();
    }

    /**
     * get record from table by input key.
     * @param key input key of the table
     * @return record of table in Record mode, null if not found.
     */
    public Record get(Key key) {
        List<Record> records = doGet(Arrays.asList(key));
        if (records == null || records.isEmpty()) {
            return null;
        }
        return records.get(0);
    }

    /**
     * get multiple records from table by input key list.
     * @param keyList input key list
     * @return record list of table in Record mode, null if not found.
     */
    public List<Record> get(List<Key> keyList) {
        return doGet(keyList);
    }

    /**
     * delete record from table by input key.
     * @param tableName name of the table
     * @param key input key of the table
     * @return true when record is deleted successfully, otherwise false.
     */
    public boolean delete(String tableName, Object[] key) {
        List<Value> userKeys = new ArrayList<>();
        for (Object keyValue: key) {
            userKeys.add(Value.get(keyValue));
        }
        Key dingoKey = new Key("DINGO", tableName, userKeys);
        return doDelete(Arrays.asList(dingoKey));
    }

    /**
     * delete record from the table by input key.
     * @param key input key
     * @return true when record is deleted successfully, otherwise false.
     */
    public boolean delete(Key key) {
        return doDelete(Arrays.asList(key));
    }

    /**
     * delete records from the table by input key list.
     * @param keyList input key list
     * @return true when records are deleted successfully, otherwise false.
     */
    public boolean delete(List<Key> keyList) {
        return doDelete(keyList);
    }

    private List<Record> doGet(List<Key> keyList) {
        ResultForClient result = storeOpUtils.doOperation(
            StoreOperationType.GET,
            keyList.get(0).getTable(),
            new ContextForClient(keyList, null, null));
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
            new ContextForClient(keyList, recordList, null));
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
            new ContextForClient(keyList, null, null));
        if (!result.getStatus()) {
            log.error("Execute put command failed:{}", result.getErrorMessage());
            return false;
        }
        return true;
    }

    public final void add(Key key, Column... columns) {
        Operation operation = Operation.add(columns);
        ContextForClient contextForClient = new ContextForClient(
            Arrays.asList(key),
            null,
            Arrays.asList(operation)
        );
        ResultForClient result = storeOpUtils.doOperation(
            StoreOperationType.COMPUTE_UPDATE,
            key.getTable(),
            contextForClient);
        if (result.getStatus() != true) {
            log.error("add operation failed, key:{}, columns:{}", key, columns);
        }
        return;
    }

    public final Record max(Key key, Column... columns) {
        Operation operation = Operation.max(columns);
        ContextForClient contextForClient = new ContextForClient(
            Arrays.asList(key),
            null,
            Arrays.asList(operation)
        );
        ResultForClient result = storeOpUtils.doOperation(
            StoreOperationType.GET_COMPUTE,
            key.getTable(),
            contextForClient);
        return result.getRecords() != null ? result.getRecords().get(0) : null;
    }

    public final Record min(Key key, Column... columns) {
        Operation operation = Operation.min(columns);
        ContextForClient contextForClient = new ContextForClient(
            Arrays.asList(key),
            null,
            Arrays.asList(operation)
        );
        ResultForClient result = storeOpUtils.doOperation(
            StoreOperationType.GET_COMPUTE,
            key.getTable(),
            contextForClient);
        return result.getRecords() != null ? result.getRecords().get(0) : null;
    }

    public final Record sum(Key key, Column... columns) {
        Operation operation = Operation.sum(columns);
        ContextForClient contextForClient = new ContextForClient(
            Arrays.asList(key),
            null,
            Arrays.asList(operation)
        );
        ResultForClient result = storeOpUtils.doOperation(
            StoreOperationType.GET_COMPUTE,
            key.getTable(),
            contextForClient);
        return result.getRecords() != null ? result.getRecords().get(0) : null;
    }

    public final void append(Key key, Column... columns) {
        Operation operation = Operation.append(columns);
        ContextForClient contextForClient = new ContextForClient(
            Arrays.asList(key),
            null,
            Arrays.asList(operation)
        );
        ResultForClient result = storeOpUtils.doOperation(
            StoreOperationType.COMPUTE_UPDATE,
            key.getTable(),
            contextForClient);
        if (!result.getStatus()) {
            log.error("append operation failed, key:{}, columns:{}", key, columns);
        }
    }

    public final void replace(Key key, Column... columns) {
        Operation operation = Operation.replace(columns);
        ContextForClient contextForClient = new ContextForClient(
            Arrays.asList(key),
            null,
            Arrays.asList(operation)
        );
        ResultForClient result = storeOpUtils.doOperation(
            StoreOperationType.COMPUTE_UPDATE,
            key.getTable(),
            contextForClient);
        if (!result.getStatus()) {
            log.error("append operation failed, key:{}, columns:{}", key, columns);
        }
    }

    /**
     * Perform multiple read/write operations on a single key in one batch call.
     *<p>
     *     Operation and ListOperation, MapOperation can be performed in same call.
     *</p>
     *
     * @param key unique record identifier
     * @param operations database operations to perform
     * @return .
     */
    public final Record operate(Key key, List<Operation> operations) {
        /*operations.stream()
            .map(op -> op.operationType.executive().execute(null, op.context));*/

        return null;
    }

    public final Record updateCol(Key key, Column... column) {
        return null;
    }
}
