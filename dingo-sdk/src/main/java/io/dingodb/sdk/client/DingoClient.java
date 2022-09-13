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

import com.google.common.collect.ImmutableList;
import io.dingodb.common.CommonId;
import io.dingodb.common.operation.Column;
import io.dingodb.common.operation.DingoExecResult;
import io.dingodb.common.operation.Operation;
import io.dingodb.common.operation.Value;
import io.dingodb.common.operation.filter.DingoFilter;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.Key;
import io.dingodb.sdk.common.Record;
import io.dingodb.sdk.operation.ContextForClient;
import io.dingodb.sdk.operation.ResultForClient;
import io.dingodb.sdk.operation.StoreOperationType;
import io.dingodb.sdk.operation.StoreOperationUtils;
import io.dingodb.sdk.operation.UDFContext;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

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

    public static Integer retryTimes = 10;
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
    public boolean open() {
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
    public void close() {
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

        String tableName = tableDef == null || tableDef.getName() == null ? "null" : tableDef.getName();
        if (tableDef == null || tableDef.getName() == null || tableDef.getName().isEmpty()) {
            log.error("Invalid TableDefinition:{}", tableDef);
            throw new DingoClientException.InvalidTableName(tableName);
        }

        copyColumnDefinition(tableDef);

        boolean isSuccess = false;
        try {
            connection.getMetaClient().createTable(tableName, tableDef);
            isSuccess = true;
            storeOpUtils.updateCacheOfTableDefinition(tableName, tableDef);
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
            throw new DingoClientException.InvalidTableName(tableName);
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
     * @return TableDefinition about table, else throw DingoClientException.InvalidTableName
     */
    public TableDefinition getTableDefinition(final String tableName) {
        if (!isConnected()) {
            log.error("connection has not been initialized, please call openConnection first");
            return null;
        }
        if (tableName == null || tableName.isEmpty()) {
            log.error("Invalid table name:{}", tableName);
            throw new DingoClientException.InvalidTableName(tableName);
        }
        TableDefinition tableDef = storeOpUtils.getTableDefinition(tableName);
        return tableDef;
    }

    /**
     * insert records into table.
     * @param tableName input table name
     * @param records record(multiple records ordered by column index on table definition).
     * @param skippedWhenExisted Whether to skip if input data exists, true is skip, false is overwritten.
     * @return true when records are inserted successfully, otherwise false.
     */
    public boolean insert(String tableName, List<Object[]> records, boolean skippedWhenExisted) {
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
        return doPut(keyList, recordList, skippedWhenExisted);
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

    /**
     * insert records into table.
     *
     * @param tableName input table name
     * @param records record(multiple records ordered by column index on table definition).
     * @return true when records are inserted successfully, otherwise false.
     */
    public boolean insert(String tableName, List<Object[]> records) {
        return insert(tableName, records, false);
    }

    private HashMap<Key, Record> convertObjectArray2Record(String tableName,
                                                           List<Object[]> recordList) {
        TableDefinition tableDefinition = storeOpUtils.getTableDefinition(tableName);
        if (tableDefinition == null) {
            log.warn("table:{} definition not found", tableName);
            throw new DingoClientException.InvalidTableName(tableName);
        }
        int expectedCount = tableDefinition.getColumns().size();
        HashMap<Key, Record>  recordResults = new LinkedHashMap<>(recordList.size());
        for (Object[] record: recordList) {
            if (record == null || record.length == 0
                || tableDefinition.getColumnsCount() != record.length) {
                log.error("Invalid record:{}, count: expect:{}, real:{}",
                    record, tableDefinition.getColumnsCount(), record != null ? record.length : 0);
                int realCnt = record != null ? record.length : 0;
                throw new DingoClientException.InvalidColumnsCnt(realCnt,  expectedCount);
            }
            List<Value> userKeys = new ArrayList<>();
            List<String> columnNames = new ArrayList<>();

            int index = 0;
            for (ColumnDefinition column : tableDefinition.getColumns()) {
                if (column.isPrimary()) {
                    if (record[index] == null) {
                        throw new DingoClientException.InvalidPrimaryKeyData();
                    }
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
            Key key = new Key(tableName, userKeys);
            Record record1 = new Record(tableDefinition.getColumns(), columns);
            recordResults.put(key, record1);
        }
        return recordResults;
    }

    /**
     * Validation and modification of column parameters when creating tables.
     */
    private static void copyColumnDefinition(TableDefinition tableDef) {
        List<ColumnDefinition> columnDefinitions = tableDef.getColumns().stream()
            .map(c ->
                ColumnDefinition.builder()
                    .name(c.getName())
                    .type(c.getType())
                    .elementType(c.getElementType())
                    .primary(c.isPrimary())
                    .scale(c.getScale())
                    .precision(c.getPrecision())
                    .defaultValue(c.getDefaultValue())
                    .notNull(c.isPrimary() || c.isNotNull())
                    .build())
            .collect(Collectors.toList());
        tableDef.setColumns(columnDefinitions);
    }

    /**
     * put record to table using key.
     * @param key input key of the table
     * @param columns input columns of the table(ordered by column index on table definition).
     * @return true when record is put successfully, otherwise false.
     */
    public boolean put(Key key, Column[] columns) {
        return put(key, columns, false);
    }

    /**
     * put record to table using key.
     * @param key input key of the table
     * @param columns input columns of the table(ordered by column index on table definition).
     * @param skippedWhenExisted Whether to skip if input data exists, true is skip, false is overwritten.
     * @return true when record is put successfully, otherwise false.
     */
    public boolean put(Key key, Column[] columns, boolean skippedWhenExisted) {
        TableDefinition tableDefinition = storeOpUtils.getTableDefinition(key.getTable());
        Record record = new Record(tableDefinition.getColumns(), columns);
        return doPut(Arrays.asList(key), Arrays.asList(record), skippedWhenExisted);
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
        return put(keyList, recordList, false);
    }

    /**
     * Note: The length of <strong>keyList</strong> and <strong>recordList</strong>MUST be equal!!!
     * put multiple records together into table.
     * @param keyList input key list of the table
     * @param recordList input value list of the table(ordered by column index on table definition).
     * @param skippedWhenExisted Whether to skip if input data exists, true is skip, false is overwritten.
     * @return true when records are put successfully, otherwise false.
     *
     */
    public boolean put(List<Key> keyList, List<Record> recordList, boolean skippedWhenExisted) {
        return doPut(keyList, recordList, skippedWhenExisted);
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
        Key dingoKey = new Key(tableName, userKeys);
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
     * Get filtered multiple records from table by entering key range.
     * @param start input start key of the table
     * @param end input end key of the table
     * @param filter filter condition
     * @return record list of table in Record mode, null if not found.
     */
    public final List<Record> query(Key start, Key end, DingoFilter filter) {
        return doQuery(Arrays.asList(start), Arrays.asList(end), filter);
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
        Key dingoKey = new Key(tableName, userKeys);
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
            ContextForClient.builder()
                .startKeyList(keyList)
                .endKeyList(Collections.emptyList())
                .skippedWhenExisted(false)
                .build());
        if (!result.getStatus()) {
            log.error("Execute get command failed:{}", result.getErrorMessage());
            return null;
        } else {
            return result.getRecords();
        }
    }

    private boolean doPut(List<Key> keyList, List<Record> recordList, boolean skippedWhenExisted) {
        ResultForClient result = storeOpUtils.doOperation(
            StoreOperationType.PUT,
            keyList.get(0).getTable(),
            ContextForClient.builder()
                .startKeyList(keyList)
                .endKeyList(Collections.emptyList())
                .recordList(recordList)
                .skippedWhenExisted(skippedWhenExisted)
                .build());

        if (!result.getStatus()) {
            log.error("Execute put command failed:{}", result.getErrorMessage());
            return false;
        }
        return true;
    }

    private List<Record> doQuery(List<Key> startKeys, List<Key> endKeys, DingoFilter filter) {
        ResultForClient result = storeOpUtils.doOperation(
            StoreOperationType.QUERY,
            startKeys.get(0).getTable(),
            ContextForClient.builder()
                .startKeyList(startKeys)
                .endKeyList(endKeys)
                .filter(filter)
                .skippedWhenExisted(false)
                .build());
        if (!result.getStatus()) {
            log.error("Execute query command failed:{}", result.getErrorMessage());
            return null;
        }
        return result.getRecords();
    }

    private boolean doDelete(List<Key> keyList) {
        ResultForClient result = storeOpUtils.doOperation(
            StoreOperationType.DELETE,
            keyList.get(0).getTable(),
            ContextForClient.builder()
                .startKeyList(keyList)
                .endKeyList(Collections.emptyList())
                .skippedWhenExisted(false)
                .build());
        if (!result.getStatus()) {
            log.error("Execute put command failed:{}", result.getErrorMessage());
            return false;
        }
        return true;
    }

    /**
     * Accumulates a column of values for a single primary key.
     * Operate on numeric types only.
     *
     * @param key primary key
     * @param columns Multiple columns to be calculated
     * @return true/false
     */
    public final boolean add(@Nonnull Key key, @Nonnull Column... columns) {
        return add(key, false, columns);
    }

    /**
     * Accumulates a column of values for a single primary key.
     * Operate on numeric types only.
     *
     * @param key primary key
     * @param useDefaultWhenNotExisted If the data does not exist, you can choose whether to use the default value
     *                                of the field when the table is defined to insert a piece of data
     * @param columns Multiple columns to be calculated
     * @return true/false
     */
    public final boolean add(@Nonnull Key key, boolean useDefaultWhenNotExisted, @Nonnull Column... columns) {
        Operation operation = Operation.add(useDefaultWhenNotExisted, columns);
        List<DingoExecResult> result = operate(key, ImmutableList.of(operation));
        return result != null && result.size() > 0 && result.get(0).isSuccess();
    }

    /**
     * Primary key range accumulates a column of values.
     * Operate on numeric types only.
     *
     * @param start primary key start position
     * @param end primary key end position
     * @param columns Multiple columns to be calculated
     * @return true/false
     */
    public final boolean add(@Nonnull Key start, @Nonnull Key end, @Nonnull Column... columns) {
        return add(start, end, false, columns);
    }

    /**
     * Primary key range accumulates a column of values.
     * Operate on numeric types only.
     *
     * @param start primary key start position
     * @param end primary key end position
     * @param useDefaultWhenNotExisted If the data does not exist, you can choose whether to use the default value
     *                                 of the field when the table is defined to insert a piece of data
     * @param columns Multiple columns to be calculated
     * @return true/false
     */
    public final boolean add(
        @Nonnull Key start,
        @Nonnull Key end,
        boolean useDefaultWhenNotExisted,
        @Nonnull Column... columns) {
        Operation operation = Operation.add(useDefaultWhenNotExisted, columns);
        List<DingoExecResult> result = operate(start, end, ImmutableList.of(operation));
        return result != null && result.size() > 0 && result.get(0).isSuccess();
    }

    /**
     * Accumulates the values matching the filter condition within the primary key range.
     * Operate on numeric types only.
     *
     * @param start primary key start position
     * @param end primary key end position
     * @param filter filter condition
     * @param columns Multiple columns to be calculated
     * @return true/false
     */
    public final boolean add(@Nonnull Key start,
                             @Nonnull Key end,
                             @Nonnull DingoFilter filter,
                             @Nonnull Column... columns) {
        Operation operation = Operation.add(false, columns);
        operation.operationContext.filter(filter);
        List<DingoExecResult> result = operate(start, end, ImmutableList.of(operation));
        return result != null && result.size() > 0 && result.get(0).isSuccess();
    }

    /**
     * Returns the maximum value within the primary key range.
     * Operate on numeric types only.
     *
     * @param start primary key start position
     * @param end primary key end position
     * @param columns Multiple columns to be calculated
     * @return The calculation result of the corresponding column
     */
    public final List<DingoExecResult> max(@Nonnull Key start, @Nonnull Key end, @Nonnull Column... columns) {
        Operation operation = Operation.max(columns);
        return operate(start, end, ImmutableList.of(operation));
    }

    /**
     * Returns the maximum value in the range of primary keys that match the filter criteria.
     * Operate on numeric types only.
     *
     * <pre> {@code
     *      DingoFilter root = new DingoFilterImpl();
     *      DingoFilter equalsFilter = new DingoValueEqualsFilter(new int[]{1}, new Object[]{10});
     *      root.addAndFilter(equalsFilter);
     * }</pre>
     *
     * @param start primary key start position
     * @param end primary key end position
     * @param filter filter condition
     * @param columns Multiple columns to be calculated
     * @return The calculation result of the corresponding column
     */
    public final List<DingoExecResult> max(@Nonnull Key start,
                                           @Nonnull Key end,
                                           @Nonnull DingoFilter filter,
                                           @Nonnull Column... columns) {
        Operation operation = Operation.max(columns);
        operation.operationContext.filter(filter);
        return operate(start, end, ImmutableList.of(operation));
    }

    /**
     * Returns the minimum value within the primary key range.
     * Operate on numeric types only.
     *
     * @param start primary key start position
     * @param end primary key end position
     * @param columns Multiple columns to be calculated
     * @return The calculation result of the corresponding column
     */
    public final List<DingoExecResult> min(@Nonnull Key start, @Nonnull Key end, @Nonnull Column... columns) {
        Operation operation = Operation.min(columns);
        return operate(start, end, ImmutableList.of(operation));
    }

    /**
     * Returns the smallest value within the range of the primary key that matches the filter criteria.
     * Operate on numeric types only.
     *
     * @param start primary key start position
     * @param end primary key end position
     * @param filter filter condition
     * @param columns Multiple columns to be calculated
     * @return The calculation result of the corresponding column
     */
    public final List<DingoExecResult> min(@Nonnull Key start,
                                           @Nonnull Key end,
                                           @Nonnull DingoFilter filter,
                                           @Nonnull Column... columns) {
        Operation operation = Operation.min(columns);
        operation.operationContext.filter(filter);
        return operate(start, end, ImmutableList.of(operation));
    }

    /**
     * Returns the total in the range of the primary key.
     * Operate on numeric types only.
     *
     * @param start primary key start position
     * @param end primary key end position
     * @param columns Multiple columns to be calculated
     * @return The calculation result of the corresponding column
     */
    public final List<DingoExecResult> sum(@Nonnull Key start, @Nonnull Key end, @Nonnull Column... columns) {
        Operation operation = Operation.sum(columns);
        return operate(start, end, ImmutableList.of(operation));
    }

    /**
     * Returns the total number of matching filter criteria within the primary key range.
     * Operate on numeric types only.
     *
     * @param start Primary key start position
     * @param end primary key end position
     * @param filter filter condition
     * @param columns Multiple columns to be calculated
     * @return The calculation result of the corresponding column
     */
    public final List<DingoExecResult> sum(@Nonnull Key start,
                                           @Nonnull Key end,
                                           @Nonnull DingoFilter filter,
                                           @Nonnull Column... columns) {
        Operation operation = Operation.sum(columns);
        operation.operationContext.filter(filter);
        return operate(start, end, ImmutableList.of(operation));
    }

    /**
     * Returns the number of rows that satisfy the filter conditions in a primary key range.
     * Operate on numeric types only.
     *
     * @param start Primary key start position
     * @param end primary key end position
     * @param filter filter condition
     * @param columns Multiple columns to be calculated
     * @return The calculation result of the corresponding column
     * @see Key
     * @see DingoExecResult
     */
    public final List<DingoExecResult> count(@Nonnull Key start,
                                             @Nonnull Key end,
                                             @Nonnull DingoFilter filter,
                                             @Nonnull Column... columns) {
        Operation operation = Operation.count(columns);
        operation.operationContext.filter(filter);
        return operate(start, end, ImmutableList.of(operation));
    }

    /**
     * Perform multiple read/write operations on a single key in one batch call.
     * @param key primary key
     * @param operations multiple read/write operations
     * @return Multiple one-to-one operation results
     */
    public final List<DingoExecResult> operate(@Nonnull Key key, @Nonnull List<Operation> operations) {
        ContextForClient contextForClient = ContextForClient.builder()
            .startKeyList(ImmutableList.of(key))
            .operationList(operations)
            .skippedWhenExisted(false)
            .build();
        return storeOpUtils.doOperation(key.getTable().toUpperCase(), contextForClient);
    }

    /**
     * Perform multiple read and write operations on the primary key range in one batch call.
     *<p>
     *     Operation and ListOperation, MapOperation can be performed in same call.
     *</p>
     *
     * @param start primary key start position
     * @param end primary key end position
     * @param operations database operations to perform
     * @return Multiple one-to-one operation results
     */
    public final List<DingoExecResult> operate(@Nonnull Key start,
                                               @Nonnull Key end,
                                               @Nonnull List<Operation> operations) {
        int startKeyCnt = start.getUserKey() != null ? start.userKey.size() : 0;
        int endKeyCnt = end.getUserKey() != null ? end.userKey.size() : 0;
        if (startKeyCnt != endKeyCnt) {
            log.error("The number of primary keys in the start:{} and end:{} ranges is different", start, end);
            throw new DingoClientException.InvalidUserKeyCnt(startKeyCnt, endKeyCnt);
        }
        ContextForClient contextForClient = ContextForClient.builder()
            .startKeyList(ImmutableList.of(start))
            .endKeyList(ImmutableList.of(end))
            .operationList(operations)
            .skippedWhenExisted(false)
            .build();
        return storeOpUtils.doOperation(start.getTable().toUpperCase(), contextForClient);
    }

    /**
     * Updates the value of a column of the specified primary key.
     *
     * @param key primary key
     * @param column column name and value
     * @return true/false
     */
    public boolean updateColumn(Key key, Column column) {
        Operation operation = Operation.update(column);
        List<DingoExecResult> result = operate(key, ImmutableList.of(operation));
        return result != null && result.size() > 0 && result.get(0).isSuccess();
    }


    /**
     * register UDF in lua format. when restart.
     * @param tableName tableName
     * @param udfName udfName
     * @param function function name
     * @return the register lua version.
     */
    public int registerUDF(String tableName, String udfName, String function) {
        CommonId id = connection.getMetaClient().getTableId(tableName);
        return connection.getMetaClient().registerUDF(id, udfName, function);
    }

    public boolean unregisterUDF(String tableName, String udfName, int version) {
        CommonId id = connection.getMetaClient().getTableId(tableName);
        return connection.getMetaClient().unregisterUDF(id, udfName, version);
    }

    public boolean updateRecordUsingUDF(String tableName, String udfName,
                                        String functionName,
                                        int version,
                                        List<Object> key) {
        List<Value> userKeys = new ArrayList<>();
        for (Object keyValue: key) {
            userKeys.add(Value.get(keyValue));
        }
        Key dingoKey = new Key(tableName, userKeys);
        UDFContext udfContext = new UDFContext(udfName, functionName, version);
        ContextForClient context = ContextForClient.builder()
            .startKeyList(ImmutableList.of(dingoKey))
            .udfContext(udfContext)
            .skippedWhenExisted(false)
            .build();
        ResultForClient result = storeOpUtils.doOperation(StoreOperationType.UPDATE_UDF, tableName, context);
        return result.getStatus();
    }

    public Record getRecordByUDF(String tableName, String udfName,
                                 String functionName,
                                 int version,
                                 List<Object> key) {
        List<Value> userKeys = new ArrayList<>();
        for (Object keyValue: key) {
            userKeys.add(Value.get(keyValue));
        }
        Key dingoKey = new Key(tableName, userKeys);
        UDFContext udfContext = new UDFContext(udfName, functionName, version);
        ContextForClient context = ContextForClient.builder()
            .startKeyList(ImmutableList.of(dingoKey))
            .udfContext(udfContext)
            .skippedWhenExisted(false)
            .build();
        ResultForClient result = storeOpUtils.doOperation(StoreOperationType.GET_UDF, tableName, context);
        return result.getRecords().get(0);
    }
}
