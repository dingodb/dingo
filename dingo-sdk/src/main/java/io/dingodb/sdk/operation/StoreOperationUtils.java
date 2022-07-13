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

package io.dingodb.sdk.operation;

import io.dingodb.common.CommonId;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.partition.RangeStrategy;
import io.dingodb.common.table.KeyValueCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.Part;
import io.dingodb.sdk.client.DingoConnection;
import io.dingodb.sdk.client.MetaClient;
import io.dingodb.sdk.client.RouteTable;
import io.dingodb.sdk.common.Key;
import io.dingodb.sdk.common.Operation;
import io.dingodb.sdk.common.Record;
import io.dingodb.server.api.ExecutorApi;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class StoreOperationUtils {

    private static Map<String, RouteTable> dingoRouteTables = new ConcurrentHashMap<>(127);
    private static Map<String, TableDefinition> tableDefinitionInCache = new ConcurrentHashMap<>(127);
    private static Map<StoreOperationType, IBaseStoreOperation> dingoOperationMap = new ConcurrentHashMap<>(3);
    private DingoConnection connection;
    private int retryTimes;

    public StoreOperationUtils(DingoConnection connection, int retryTimes) {
        this.connection = connection;
        this.retryTimes = retryTimes;
        initStoreOperation();
    }

    public boolean executeRemoteOperation(StoreOperationType type, String tableName, Key key, Record record) {
        RouteTable routeTable = getAndRefreshRouteTable(tableName, false);
        if (routeTable == null) {
            log.error("table {} not found when do operation:{}", tableName, type);
            return false;
        }
        boolean isSuccess = false;
        int retryTimes = this.retryTimes;
        do {
            try {
                KeyValueCodec codec = routeTable.getCodec();
                byte[] keyInBytes = null; // codec.encodeKey(key.getUserKey());
                byte[] valueInBytes = null; //codec.encode(record.columns.values().toArray(new Object[0]));
                ExecutorApi executorApi = getExecutor(tableName, keyInBytes);

                IBaseStoreOperation storeOp = getDingoStoreOp(type);
                isSuccess = storeOp.doOperation(executorApi, routeTable.getTableId(), keyInBytes, valueInBytes);
            } catch (Exception ex) {
                log.error("executeRemoteOperation error", ex.toString(), ex);
            } finally {
                if (!isSuccess && retryTimes > 0) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                    routeTable = getAndRefreshRouteTable(tableName, true);
                }
            }
        }
        while (!isSuccess && --retryTimes > 0);
        return isSuccess;
    }

    public Record executeRemoteCompute(StoreOperationType type, String tableName, Key key, Operation operation) {
        RouteTable routeTable = getAndRefreshRouteTable(tableName, false);
        if (routeTable == null) {
            log.error("table {} not found when do operation:{}", tableName, type);
            return null;
        }

        int retryTimes = this.retryTimes;
        do {
            try {
                KeyValueCodec codec = routeTable.getCodec();
                byte[] keyInBytes = null; // codec.encodeKey(key.getUserKey());
                if (operation.type.isWrite) {
                    ExecutorApi executorApi = getExecutor(tableName, keyInBytes);

                    IBaseStoreOperation storeOp = getDingoStoreOp(type);
                    return storeOp.doCompute(
                        executorApi,
                        routeTable.getTableId(),
                        keyInBytes,
                        ProtostuffCodec.write(operation));

                } else {
                    // todo scan --> operation.type.execute() --> record
                    // collection --> for - operation.type.execute()
                }
            } catch (Exception ex) {
                log.error("executeRemoteOperation error", ex.toString(), ex);
            } finally {
                if (retryTimes > 0) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    routeTable = getAndRefreshRouteTable(tableName, true);
                }
            }
        }
        while (--retryTimes > 0);
        return null;
    }

    /**
     * get route table from coordinator or cache, and refresh it if need.
     * @param tableName input tableName
     * @param isRefresh if isRefresh == true, then refresh route table
     * @return RouteTable about table partition leaders and replicas on hosts.
     */
    public RouteTable getAndRefreshRouteTable(final String tableName, boolean isRefresh) {
        if (isRefresh) {
            dingoRouteTables.remove(tableName);
        }
        RouteTable routeTable = dingoRouteTables.get(tableName);
        if (routeTable == null) {
            MetaClient metaClient = connection.getMetaClient();
            CommonId tableId = metaClient.getTableId(tableName);
            TableDefinition tableDef = metaClient.getTableDefinition(tableName);
            if (tableDef == null) {
                log.error("Cannot find table:{} defination from meta", tableName);
                return null;
            }
            tableDefinitionInCache.put(tableName, tableDef);
            NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions = metaClient.getParts(tableName);
            KeyValueCodec keyValueCodec = new KeyValueCodec(tableDef.getTupleSchema(), tableDef.getKeyMapping());
            RangeStrategy rangeStrategy = new RangeStrategy(tableDef, partitions.navigableKeySet());
            TreeMap<ByteArrayUtils.ComparableByteArray, ExecutorApi> partitionExecutor = new TreeMap<>();
            routeTable = new RouteTable(
                tableName,
                tableId,
                keyValueCodec,
                partitions,
                partitionExecutor,
                rangeStrategy);
            dingoRouteTables.put(tableName, routeTable);
        }
        return routeTable;
    }

    /**
     * get column name in order by index from table definition.
     * @param tableName input table name.
     * @return column name in order by index.
     */
    public synchronized List<String> getColumnNamesInOrder(String tableName) {
        TableDefinition tableDef = tableDefinitionInCache.get(tableName);
        if (tableDef == null) {
            MetaClient metaClient = connection.getMetaClient();
            tableDef = metaClient.getTableDefinition(tableName);
            if (tableDef != null) {
                tableDefinitionInCache.put(tableName, tableDef);
            }
        }

        if (tableDef == null) {
            log.error("Cannot find table:{} definition from meta", tableName);
            return null;
        }
        return tableDef.getColumns().stream().map(c -> c.getName()).collect(Collectors.toList());
    }

    /**
     * update table definition into local cache.
     * @param tableName table name.
     * @param tableDef table definition.
     */
    public synchronized void updateCacheOfTableDefinition(final String tableName, final TableDefinition tableDef) {
        if (tableName != null && !tableName.isEmpty() && tableDef != null) {
            tableDefinitionInCache.put(tableName, tableDef);
            log.info("update cache of table:{} definition:{}", tableName, tableDef);
        }
    }

    public synchronized void initStoreOperation() {
        dingoOperationMap.put(StoreOperationType.PUT, new PutRecordOperation());
        dingoOperationMap.put(StoreOperationType.COMPUTE, new ComputeOperation());
    }

    /**
     * remove table definition from local cache.
     * @param tableName input table name.
     */
    public synchronized void removeCacheOfTableDefinition(String tableName) {
        if (tableName != null) {
            TableDefinition tableDefinition = tableDefinitionInCache.remove(tableName);
            if (tableDefinition != null) {
                log.info("remove cache of table:{} definition:{}", tableName, tableDefinition);
            }
        }
    }

    public static Map<String, TableDefinition> getTableDefinitionInCache() {
        return tableDefinitionInCache;
    }

    public synchronized void clearTableDefinitionInCache() {
        tableDefinitionInCache.clear();
        return;
    }

    private synchronized ExecutorApi getExecutor(final String tableName, byte[] keyInBytes) {
        RouteTable executionTopology = getAndRefreshRouteTable(tableName, false);
        if (executionTopology == null) {
            log.warn("Cannot find execution topology for table:{}", tableName);
            return null;
        }
        ExecutorApi executorApi = executionTopology.getOrUpdateLocationByKey(
            connection.getApiRegistry(),
            keyInBytes);
        return executorApi;
    }

    private IBaseStoreOperation getDingoStoreOp(StoreOperationType type) {
        return dingoOperationMap.get(type);
    }
}
