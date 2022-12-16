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
import io.dingodb.common.DingoOpResult;
import io.dingodb.common.codec.DingoKeyValueCodec;
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.partition.RangeStrategy;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.Part;
import io.dingodb.sdk.client.DingoConnection;
import io.dingodb.sdk.client.MetaClient;
import io.dingodb.sdk.client.RouteTable;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.Key;
import io.dingodb.sdk.operation.op.Op;
import io.dingodb.server.api.ExecutorApi;
import io.dingodb.server.protocol.meta.Schema;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Future;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.ROOT_DOMAIN;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

@Slf4j
public class StoreOperationUtils {

    private static Map<String, RouteTable> dingoRouteTables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private static Map<String, TableDefinition> tableDefinitionInCache = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private DingoConnection connection;
    private int retryTimes;

    public StoreOperationUtils(DingoConnection connection, int retryTimes) {
        this.connection = connection;
        this.retryTimes = retryTimes;
    }

    public void shutdown() {
        clearTableDefinitionInCache();
    }

    public DingoOpResult doOp(Op op) {
        Op head = Parameters.cleanNull(op.head(), op);
        String tableName = head.context().startPrimaryKeys.get(0).getTable();
        RouteTable routeTable = getAndRefreshRouteTable(tableName, false);
        if (routeTable == null) {
            log.error("table {} not found when do operation", tableName);
            return null;
        }
        DingoOpResult result = null;
        try {
            TableDefinition tableDefinition = getTableDefinition(tableName);

            head.context().definition(tableDefinition);

            ContextForStore storeContext = ContextForStore.builder()
                .startKeyListInBytes(head.context().startKey())
                .endKeyListInBytes(head.context().endKey())
                .operationListInBytes(ProtostuffCodec.write(head))
                .build();
            Map<String, ContextForStore> keys2Exec = groupKeysByExecutor(routeTable, null, tableName, storeContext);
            List<Future<Object>> futures = new ArrayList<>();
            for (Map.Entry<String, ContextForStore> entry : keys2Exec.entrySet()) {
                String leaderAddress = entry.getKey();
                ContextForStore forStore = entry.getValue();
                ExecutorApi executorApi = getExecutor(routeTable, leaderAddress);
                Future<Object> future = executorApi.operator(
                    routeTable.getTableId(),
                    forStore.getStartKeyListInBytes(),
                    forStore.getEndKeyListInBytes() == null ? null : forStore.getEndKeyListInBytes(),
                    forStore.getOperationListInBytes());
                futures.add(future);
            }
            for (Future<Object> future : futures) {
                DingoOpResult r = (DingoOpResult) future.get();
                if (result == null) {
                    result = r;
                } else {
                    result = result.merge(r.getValue());
                }
            }
        } catch (Exception e) {
            log.info("exec failed, e: ", e);
        }
        return result;
    }

    public Object[] get(String tableName, Key key) {
        RouteTable routeTable = getAndRefreshRouteTable(tableName, false);
        if (routeTable == null) {
            log.error("table {} not found when do get", tableName);
            return null;
        }

        TableDefinition tableDefinition = getTableDefinition(tableName);
        try {
            byte[] encodeKey = routeTable.getCodec().encodeKey(key.getUserKey().toArray());
            String leaderAddress = getLeaderAddressByStartKey(routeTable, encodeKey);
            ExecutorApi executorApi = getExecutor(routeTable, leaderAddress);
            CommonId schema = new CommonId(
                ID_TYPE.table, TABLE_IDENTIFIER.schema, ROOT_DOMAIN, ROOT_DOMAIN
            );
            byte[] value = executorApi.getValueByPrimaryKey(null,
                schema, routeTable.getTableId(), encodeKey);
            Object[] objects = routeTable.getCodec().decodeKey(value);
            return objects;
        } catch (IOException e) {
            log.info("get failed, e: ", e);
        }

        return null;
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
                TableDefinition tableDefinition = getTableDefinition(tableName);
                IStoreOperation storeOperation = StoreOperationFactory.getStoreOperation(type);
                ContextForStore context4Store = Converter.getStoreContext(storeParameters, codec, tableDefinition);
                Map<String, ContextForStore> keys2Executor = groupKeysByExecutor(
                    routeTable,
                    type,
                    tableName,
                    context4Store);

                List<Future<ResultForStore>> futureArrayList = new ArrayList<>();
                for (Map.Entry<String, ContextForStore> entry : keys2Executor.entrySet()) {
                    String leaderAddress = entry.getKey();
                    ExecutorApi executorApi = getExecutor(routeTable, leaderAddress);
                    futureArrayList.add(
                        Executors.submit("do-operation",
                            new CallableTask(
                                executorApi,
                                storeOperation,
                                routeTable.getTableId(),
                                entry.getValue()
                            )
                        )
                    );
                }

                List<KeyValue> keyValueList = new ArrayList<>();
                for (Future<ResultForStore> subFutureResult: futureArrayList) {
                    ResultForStore subResult4Store = subFutureResult.get();
                    isSuccess = subResult4Store.getStatus();
                    if (!isSuccess) {
                        errorMsg = subResult4Store.getErrorMessage();
                        throw new DingoClientException(errorMsg);
                    }
                    if (subResult4Store.getRecords() != null && subResult4Store.getRecords().size() > 0) {
                        keyValueList.addAll(subResult4Store.getRecords());
                    }
                }
                ResultForStore result4Store = new ResultForStore(isSuccess, errorMsg, keyValueList);
                result4Client = Converter.getResultCode(
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
                        Thread.sleep(200);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                    routeTable = getAndRefreshRouteTable(tableName, true);
                }
            }
        }
        while (!isSuccess && --retryTimes > 0);

        if (retryTimes == 0 || !isSuccess) {
            log.error("Execute operation:{} on table:{} failed, retry times:{}", type, tableName, retryTimes);
        }
        return result4Client;
    }

    /**
     * group all keys by Executor.
     * @param type operation type.
     * @param tableName tableName
     * @param wholeContext  all records contains key and other columns.
     * @return Executor(StartKey of Partition only used for group by)->ContextForStore
     */
    private Map<String, ContextForStore> groupKeysByExecutor(
        RouteTable routeTable,
        StoreOperationType type,
        String tableName,
        ContextForStore wholeContext) {
        Map<String, List<byte[]>> keyListByExecutor = new TreeMap<>();
        for (int index = 0; index < wholeContext.getStartKeyListInBytes().size(); index++) {
            byte[] keyInBytes = wholeContext.getStartKeyListInBytes().get(index);
            String leaderAddress = getLeaderAddressByStartKey(routeTable, keyInBytes);
            if (leaderAddress == null) {
                log.error("Cannot find partition, table {} key:{} not found when do operation:{}",
                    tableName,
                    Arrays.toString(keyInBytes),
                    type);
                throw new DingoClientException("table " + tableName + " key:" + Arrays.toString(keyInBytes)
                    + " not found when do operation:" + type);
            }
            List<byte[]> keyList = keyListByExecutor.get(leaderAddress);
            if (keyList == null) {
                keyList = new java.util.ArrayList<>();
                keyListByExecutor.put(leaderAddress, keyList);
            }
            keyList.add(keyInBytes);
        }

        Map<String, ContextForStore> contextGroupyByExecutor = new TreeMap<>();
        for (Map.Entry<String, List<byte[]>> entry : keyListByExecutor.entrySet()) {
            String leaderAddress = entry.getKey();
            List<byte[]> keys = entry.getValue();
            List<KeyValue> records = new java.util.ArrayList<>();
            for (byte[] key : keys) {
                records.add(wholeContext.getRecordByKey(key));
            }
            ContextForStore subStoreContext = ContextForStore.builder()
                .startKeyListInBytes(keys)
                .endKeyListInBytes(wholeContext.getEndKeyListInBytes())
                .recordList(records)
                .operationListInBytes(wholeContext.getOperationListInBytes())
                .udfContext(wholeContext.getUdfContext())
                .skippedWhenExisted(wholeContext.isSkippedWhenExisted())
                .build();
            contextGroupyByExecutor.put(leaderAddress, subStoreContext);
        }
        return contextGroupyByExecutor;
    }

    /**
     * get route table from coordinator or cache, and refresh it if need.
     * @param tableName input tableName
     * @param isRefresh if isRefresh == true, then refresh route table
     * @return RouteTable about table partition leaders and replicas on hosts.
     */
    public synchronized RouteTable getAndRefreshRouteTable(final String tableName, boolean isRefresh) {
        if (isRefresh) {
            dingoRouteTables.remove(tableName);
        }
        RouteTable routeTable = dingoRouteTables.get(tableName);
        if (routeTable == null) {
            MetaClient metaClient = connection.getMetaClient();
            TableDefinition tableDefinition = metaClient.getTableDefinition(tableName.toUpperCase());
            if (tableDefinition == null) {
                log.error("Cannot find table:{} definition from meta", tableName);
                return null;
            }
            tableDefinitionInCache.put(tableName, tableDefinition);
            NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions =
                metaClient.getParts(tableDefinition.getName());
            KeyValueCodec keyValueCodec = new DingoKeyValueCodec(
                tableDefinition.getDingoType(),
                tableDefinition.getKeyMapping()
            );
            RangeStrategy rangeStrategy = new RangeStrategy(tableDefinition, partitions.navigableKeySet());
            CommonId tableId = metaClient.getTableId(tableDefinition.getName());
            routeTable = new RouteTable(
                tableName,
                tableId,
                keyValueCodec,
                partitions,
                rangeStrategy);
            dingoRouteTables.put(tableName, routeTable);
            if (log.isDebugEnabled()) {
                log.debug("Refresh route table:{}, tableDef:{}", tableName, tableDefinition);
            }
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
            tableDef = metaClient.getTableDefinition(tableName.toUpperCase());
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
            dingoRouteTables.remove(tableName);
            if (tableDefinition != null) {
                log.info("remove cache of table:{} definition:{}", tableName, tableDefinition);
            }
        }
    }

    public static Map<String, TableDefinition> getTableDefinitionInCache() {
        return tableDefinitionInCache;
    }

    private synchronized void clearTableDefinitionInCache() {
        tableDefinitionInCache.clear();
        return;
    }

    private synchronized ExecutorApi getExecutor(final RouteTable routeTable, String leaderAddress) {
        ExecutorApi executorApi = routeTable.getLeaderAddress(connection.getApiRegistry(), leaderAddress);
        return executorApi;
    }

    private synchronized String getLeaderAddressByStartKey(final RouteTable routeTable, byte[] keyInBytes) {
        return routeTable.getStartPartitionKey(connection.getApiRegistry(), keyInBytes);
    }
}
