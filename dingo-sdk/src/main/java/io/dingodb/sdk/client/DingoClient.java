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
import io.dingodb.sdk.common.Record;
import io.dingodb.sdk.operation.StoreOperationType;
import io.dingodb.sdk.operation.StoreOperationUtils;
import lombok.extern.slf4j.Slf4j;

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
            connection.getMetaClient().dropTable(tableName);
            isSuccess = true;
            storeOpUtils.removeCacheOfTableDefinition(tableName);
        } catch (Exception e) {
            isSuccess = false;
            log.error("drop table: {} failed:{}", tableName, e.toString(), e);
        }
        return isSuccess;
    }


    public boolean put(Key key, Record record) throws Exception {
        return interalPutRecord(key, record);
    }

    public boolean put(Key key, Column[] columns) throws Exception {
        // convert columns to record
        List<String> columnsInOrder = storeOpUtils.getColumnNamesInOrder(key.getTable());
        Record record = new Record(columnsInOrder, columns);
        return interalPutRecord(key, record);
    }

    public boolean put(List<Key> keyList, List<Record> recordList) throws Exception {
        /**
         * should group by key, and then do batch put.
         */
        return true;
    }

    public Record get(Key key) throws Exception {
        return null;
    }

    public List<Record> get(List<Key> keyList) throws Exception {
        return null;
    }

    public boolean delete(Key key) throws Exception {
        return false;
    }

    public boolean delete(List<Key> keyList) throws Exception {
        return false;
    }

    private boolean interalPutRecord(Key key, Record record) {
        storeOpUtils.executeRemoteOperation(StoreOperationType.PUT, key.getTable(), key, record);
        return false;
    }

    public Record operate(Key key, Operation operation) {
        return storeOpUtils.executeRemoteCompute(StoreOperationType.COMPUTE, key.getTable(), key, operation);
    }

    public Record updateCol(Key key, Column... column) {

        return null;
    }
}
