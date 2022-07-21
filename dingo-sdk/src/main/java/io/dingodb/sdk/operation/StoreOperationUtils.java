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
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.partition.RangeStrategy;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.DingoKeyValueCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.Part;
import io.dingodb.sdk.client.DingoConnection;
import io.dingodb.sdk.client.MetaClient;
import io.dingodb.sdk.client.RouteTable;
import io.dingodb.sdk.utils.DingoClientException;
import io.dingodb.server.api.ExecutorApi;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class StoreOperationUtils {

    private static Map<String, RouteTable> dingoRouteTables = new ConcurrentHashMap<>(127);
    private static Map<String, TableDefinition> tableDefinitionInCache = new ConcurrentHashMap<>(127);
    private DingoConnection connection;
    private int retryTimes;

    public StoreOperationUtils(DingoConnection connection, int retryTimes) {
        this.connection = connection;
        this.retryTimes = retryTimes;
    }

    public ResultForClient doOperation(StoreOperationType type,
                                       String tableName,
                                       ContextForClient storeParameters) {
        RouteTable routeTable = getAndRefreshRouteTable(tableName, false);
        if (routeTable == null) {
            log.error("table {} not found when do operation:{}", tableName, type);
            return new ResultForClient(false, null);
        }

        boolean isSuccess = false;
        String errorMsg = "";
        int retryTimes = this.retryTimes;
        ResultForClient result4Client = null;
        do {
            try {
                KeyValueCodec codec = routeTable.getCodec();
                IStoreOperation storeOperation = StoreOperationFactory.getStoreOperation(type);
                ContextForStore context4Store = ConvertUtils.getStoreContext(storeParameters, codec);
                Map<ByteArrayUtils.ComparableByteArray, ContextForStore> keys2Executor =
                    groupKeysByExecutor(type, tableName, context4Store);

                List<KeyValue> keyValueList = new ArrayList<>();
                // FIXME: will be execute in parallel(Huzx)
                for (Map.Entry<ByteArrayUtils.ComparableByteArray, ContextForStore> entry : keys2Executor.entrySet()) {
                    byte[] startKeyInBytes = entry.getValue().getKeyListInBytes().get(0);
                    ExecutorApi executorApi = getExecutor(tableName, startKeyInBytes);
                    ResultForStore subResult = storeOperation.doOperation(
                        executorApi, routeTable.getTableId(), entry.getValue());
                    isSuccess = subResult.getStatus();
                    if (!isSuccess) {
                        errorMsg = subResult.getErrorMessage();
                        log.error("do operation:{} failed, table:{}, executor:{}, result:{}",
                            type, tableName, executorApi, subResult);
                    } else {
                        if (subResult.getRecords() != null) {
                            keyValueList.addAll(subResult.getRecords());
                        }
                    }
                }
                ResultForStore result4Store = new ResultForStore(isSuccess, errorMsg, keyValueList);
                result4Client = ConvertUtils.getResultCode(
                    result4Store,
                    codec,
                    getTableDefinition(tableName).getColumns());
            } catch (Exception ex) {
                errorMsg = "Execute operation:" + type + " failed, retry times:" + retryTimes;
                log.error(errorMsg, ex);
                result4Client = new ResultForClient(false, errorMsg);
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
        return result4Client;
    }

    /**
     * group all keys by Executor.
     * @param type operation type.
     * @param tableName tableName
     * @param wholeContext  all records contains key and other columns.
     * @return Executor(HashCode only used for group by)->ContextForStore
     */
    private Map<ByteArrayUtils.ComparableByteArray, ContextForStore> groupKeysByExecutor(
        StoreOperationType type, String tableName, ContextForStore wholeContext) {
        Map<ByteArrayUtils.ComparableByteArray, List<byte[]>> keyListByExecutor = new TreeMap<>();
        for (int index = 0; index < wholeContext.getKeyListInBytes().size(); index++) {
            byte[] keyInBytes = wholeContext.getKeyListInBytes().get(index);
            ByteArrayUtils.ComparableByteArray startKeyOfPart = getPartitionStartKey(tableName, keyInBytes);
            if (startKeyOfPart == null) {
                log.error("Cannot find partition, table {} key:{} not found when do operation:{}",
                    tableName,
                    Arrays.toString(keyInBytes),
                    type);
                throw new DingoClientException("table " + tableName + " key:" + Arrays.toString(keyInBytes)
                    + " not found when do operation:" + type);
            }
            List<byte[]> keyList = keyListByExecutor.get(startKeyOfPart);
            if (keyList == null) {
                keyList = new java.util.ArrayList<>();
                keyListByExecutor.put(startKeyOfPart, keyList);
            }
            keyList.add(keyInBytes);
        }

        Map<ByteArrayUtils.ComparableByteArray, ContextForStore> contextByExecutor = new TreeMap<>();
        for (Map.Entry<ByteArrayUtils.ComparableByteArray, List<byte[]>> entry : keyListByExecutor.entrySet()) {
            ByteArrayUtils.ComparableByteArray partitionStartKey = entry.getKey();
            List<byte[]> keys = entry.getValue();
            List<KeyValue> records = new java.util.ArrayList<>();
            for (byte[] key : keys) {
                KeyValue keyValue = wholeContext.getRecordByKey(key);
                records.add(wholeContext.getRecordByKey(key));
            }
            ContextForStore subStoreContext = new ContextForStore(
                keys,
                records,
                wholeContext.getOperationListInBytes());
            contextByExecutor.put(partitionStartKey, subStoreContext);
            log.info("After Group Keys by Executor: subGroup=>keyCnt:{} reocordCnt:{}", keys.size(), records.size());
        }
        return contextByExecutor;
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
            KeyValueCodec keyValueCodec = new DingoKeyValueCodec(tableDef.getDingoType(), tableDef.getKeyMapping());
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
    public synchronized TableDefinition getTableDefinition(String tableName) {
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
        return tableDef;
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

    private synchronized ByteArrayUtils.ComparableByteArray getPartitionStartKey(final String tableName,
                                                                                 byte[] keyInBytes) {
        RouteTable executionTopology = getAndRefreshRouteTable(tableName, false);
        if (executionTopology == null) {
            log.warn("Cannot find execution topology for table:{}", tableName);
            return null;
        }
        return executionTopology.getStartPartitionKey(connection.getApiRegistry(), keyInBytes);
    }
}
