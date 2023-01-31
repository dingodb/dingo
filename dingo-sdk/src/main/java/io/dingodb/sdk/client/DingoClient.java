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
import io.dingodb.common.DingoOpResult;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.MetaService;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.Key;
import io.dingodb.sdk.common.Record;
import io.dingodb.sdk.operation.Column;
import io.dingodb.sdk.operation.ContextForClient;
import io.dingodb.sdk.operation.ResultForClient;
import io.dingodb.sdk.operation.StoreOperationType;
import io.dingodb.sdk.operation.StoreOperationUtils;
import io.dingodb.sdk.operation.UDFContext;
import io.dingodb.sdk.operation.Value;
import io.dingodb.sdk.operation.filter.DingoFilter;
import io.dingodb.sdk.operation.op.Op;
import io.dingodb.sdk.operation.op.impl.CollectionOp;
import io.dingodb.sdk.operation.op.impl.WriteOp;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    private final DingoConnection connection;

    public String user;

    public String password;

    private StoreOperationUtils storeOpUtils;

    public static Integer retryTimes = 30;
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
        this(coordinatorExchangeSvrList, MetaService.DINGO_NAME, retryTimes);
    }

    public DingoClient(String coordinatorExchangeSvrList, String schema, Integer retryTimes) {
        try {
            connection = new DingoConnection(coordinatorExchangeSvrList, schema, retryTimes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        DingoClient.retryTimes = retryTimes;
    }

    public void setIdentity(String user, String password) {
        this.user = user;
        this.password = password;
    }


    /**
     * build connection to dingo database cluster.
     * @return true when connection is built successfully, otherwise false.
     */
    public boolean open() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setRole(DingoRole.SDK_CLIENT);
            if (StringUtils.isBlank(user)) {
                this.user = "";
                this.password = "";
            }
            env.setInfo("user", user);
            env.setInfo("password", password);
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
                e, e);
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
            log.error("drop table: {} failed:{}", tableName, e, e);
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
     * @param record ordered by column index on table definition.
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
     */
    public boolean insert(String tableName, List<Object[]> records) {
        return insert(tableName, records, false);
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
            log.error("Invalid input rowList:{}", records);
            return false;
        }
        HashMap<Key, Record> recordMap = convertObjectArray2Record(tableName, records);
        List<Key> keyList = new ArrayList<>();
        List<Record> recordList = new ArrayList<>();
        for (Map.Entry<Key, Record> entry : recordMap.entrySet()) {
            keyList.add(entry.getKey());
            recordList.add(entry.getValue());
        }
        TableDefinition definition = getTableDefinition(tableName);
        if (definition.getIndexes().isEmpty()) {
            WriteOp op = Op.put(keyList, recordList, skippedWhenExisted);
            DingoOpResult result = exec(op);
            return (boolean) result.getValue();
        } else {
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
                    .type(c.getTypeName())
                    .elementType(c.getElementType())
                    .primary(c.getPrimary())
                    .scale(c.getScale())
                    .precision(c.getPrecision())
                    .defaultValue(c.getDefaultValue())
                    .nullable(c.isNullable())
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
     * Update the values of multiple columns for a single row of records.
     * By default, the default value is not used when the record does not exist
     * @param key input key of the table
     * @param columns Column names and values to update
     * @return Return true if the record was updated successfully, false otherwise
     */
    public boolean update(Key key, Column[] columns) {
        return update(key, columns, false);
    }

    /**
     * Update the values of multiple columns for a single row of records.
     *
     * @param key input key of the table
     * @param columns Column names and values to update
     * @param useDefaultWhenNotExisted
     *      Whether to use default values to add records and update column values when records do not exist
     * @return Return true if the record was updated successfully, false otherwise
     */
    public boolean update(Key key, Column[] columns, boolean useDefaultWhenNotExisted) {
        return update(Collections.singletonList(key), columns, useDefaultWhenNotExisted);
    }

    /**
     * Update the values of multiple columns for a single row of records.
     * By default, the default value is not used when the record does not exist
     * @param keyList input key list
     * @param columns Column names and values to update
     * @param useDefaultWhenNotExisted
     *      Whether to use default values to add records and update column values when records do not exist
     * @return Return true if the record was updated successfully, false otherwise
     */
    public boolean update(List<Key> keyList, Column[] columns, boolean useDefaultWhenNotExisted) {
        Op op = Op.update(keyList, columns, useDefaultWhenNotExisted);
        DingoOpResult result = exec(op);
        return (boolean) result.getValue();
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
        if (dingoRecord == null) {
            return null;
        }
        return dingoRecord.getDingoColumnValuesInOrder();
    }

    /**
     * get record from table by input key.
     * @param key input key of the table
     * @return record of table in Record mode, null if not found.
     */
    public Record get(Key key) {
        List<Record> records = doGet(Arrays.asList(key));
        if (records.isEmpty()) {
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
        return doQuery(start, end, filter);
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
        TableDefinition definition = getTableDefinition(keyList.get(0).getTable());
        if (definition.getIndexes().isEmpty()) {
            WriteOp op = Op.put(keyList, recordList, skippedWhenExisted);
            DingoOpResult result = exec(op);
            return (boolean) result.getValue();
        } else {
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
    }

    private List<Record> doQuery(Key startKey, Key endKey, DingoFilter filter) {
        CollectionOp op = Op.scan(startKey, endKey).filter(filter);
        DingoOpResult result = exec(op);

        return toRecord(result, startKey.getTable());
    }

    private boolean doDelete(List<Key> keyList) {
        Op delete = Op.delete(keyList);
        DingoOpResult result = exec(delete);

        return (boolean) result.getValue();
    }

    private List<Record> toRecord(DingoOpResult opResult, String tableName) {
        TableDefinition definition = storeOpUtils.getTableDefinition(tableName);
        List<Record> records = new ArrayList<>();
        Iterator<Object[]> iterator = (Iterator<Object[]>) opResult.getValue();
        while (iterator.hasNext()) {
            List<Column> columnArray = new ArrayList<>();
            Object[] obj = iterator.next();
            for (int i = 0; i < obj.length; i++) {
                Column column = new Column(definition.getColumns().get(i).getName(), obj[i]);
                columnArray.add(column);
            }
            records.add(new Record(definition.getColumns(), columnArray.toArray(new Column[0])));
        }
        return records;
    }

    public final DingoOpResult exec(Op operation) {
        return storeOpUtils.doOp(operation);
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
        return connection.getMetaClient().register(udfName, function);
    }

    public boolean unregisterUDF(String tableName, String udfName, int version) {
        connection.getMetaClient().unregister(udfName, version);
        return true;
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

    /**
     * get table record by input key.
     * @param tableName input table name
     * @param key input key of the table
     * @return record of the table in array of object, null if not found.
     */
    public Object[] getWithVerify(String tableName, Object[] key) {
        List<Value> userKeys = new ArrayList<>();
        for (Object keyValue: key) {
            userKeys.add(Value.get(keyValue));
        }
        Key dingoKey = new Key(tableName, userKeys);
        return storeOpUtils.get(tableName, dingoKey);
    }
}
