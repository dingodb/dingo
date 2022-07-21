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

import io.dingodb.common.table.TableDefinition;
import io.dingodb.sdk.common.Column;
import io.dingodb.sdk.common.Key;
import io.dingodb.sdk.common.Operation;
import io.dingodb.sdk.common.OperationType;
import io.dingodb.sdk.common.Record;
import io.dingodb.sdk.operation.ContextForClient;
import io.dingodb.sdk.operation.ResultForClient;
import io.dingodb.sdk.operation.StoreOperationType;
import io.dingodb.sdk.operation.StoreOperationUtils;
import io.dingodb.sdk.utils.DingoClientException;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;

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


    public DingoClient(String configPath) {
        this(configPath, retryTimes);
    }

    public DingoClient(String coordinatorExchangeSvrList, String currentHost) {
        this(coordinatorExchangeSvrList, currentHost, 9999, retryTimes);
    }

    public DingoClient(String configPath, int retryTimes) {
        connection = new DingoConnection(configPath);
        this.retryTimes = retryTimes;
    }

    public DingoClient(String coordinatorExchangeSvrList,
                       String currentHost,
                       Integer currentPort,
                       Integer retryTimes) {
        connection = new DingoConnection(coordinatorExchangeSvrList, currentHost, currentPort);
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


    public boolean put(Key key, Record record) throws DingoClientException {
        return doPut(Arrays.asList(key), Arrays.asList(record));
    }

    public boolean put(Key key, Column[] columns) throws DingoClientException {
        TableDefinition tableDefinition = storeOpUtils.getTableDefinition(key.getTable());
        Record record = new Record(tableDefinition.getColumns(), columns);
        return doPut(Arrays.asList(key), Arrays.asList(record));
    }

    public boolean put(List<Key> keyList, List<Record> recordList) throws DingoClientException {
        return doPut(keyList, recordList);
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
