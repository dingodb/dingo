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

package io.dingodb.index;

import io.dingodb.common.CommonId;
import io.dingodb.common.codec.DingoKeyValueCodec;
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.partition.RangeStrategy;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.index.api.CoordinatorServerApi;
import io.dingodb.index.api.ExecutorServerApi;
import io.dingodb.index.api.TableCoordinatorServerApi;
import io.dingodb.meta.Part;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.sdk.client.ClientBase;
import io.dingodb.sdk.client.RouteTable;
import io.dingodb.server.api.ExecutorApi;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class DingoDmlExecutor extends ClientBase {

    private NetService netService;
    @Getter
    private CoordinatorConnector coordinatorConnector;

    private MetaServiceApi metaServiceApi = super.getNetService()
            .apiRegistry().proxy(MetaServiceApi.class, super.getCoordinatorConnector());

    private ApiRegistry apiRegistry = super.getNetService().apiRegistry();

    CoordinatorServerApi coordinatorServerApi;
    Map<String, TableCoordinatorServerApi> tableCoordinatorServerApiMap = new HashMap<>();

    public DingoDmlExecutor(String coordinatorExchangeSvrList) {
        super(coordinatorExchangeSvrList);
    }

    public void init(String coordinatorAddr) {
        //使用配置的coordinator地址初始化coordinatorServerApi
        coordinatorServerApi = null;
    }

    public void executeInsert(String tableName, Object[] record) throws Exception {
        // 1. 获取该Table对应的tableCoordinator服务地址
        String tableCoordinatorAddr = coordinatorServerApi.getTableCoordinatorAddr(tableName);
        // 2. 使用tableCoordinatorAddr初始化tableCoordinatorServerApi，优先使用缓存
        if (!tableCoordinatorServerApiMap.containsKey(tableName)) {
            TableCoordinatorServerApi tableCoordinatorServerApi = null;
            tableCoordinatorServerApiMap.put(tableName, tableCoordinatorServerApi);
        }
        TableCoordinatorServerApi tableCoordinatorServerApi = tableCoordinatorServerApiMap.get(tableName);
        // 3. 获取TableDefinition
        TableDefinition tableDefinition = tableCoordinatorServerApi.getTableDefinition(tableName);
        // 4. 序列化主键索引及其他索引数据
        KeyValueCodec keyValueCodec = null;
        Map<String, KeyValueCodec> allIndexKeyValueCodec = null;

        // 获得index keyvaluecodec

        tableDefinition.getIndexesMapping();





        KeyValue keyValue = keyValueCodec.encode(record);
        Map<String, KeyValue> allIndexKeyValue = new HashMap<>();
        for (String indexName : allIndexKeyValueCodec.keySet()) {
            allIndexKeyValue.put(indexName, allIndexKeyValueCodec.get(indexName).encode(record));
        }

        // 5. 获取所有索引包含主键的executor地址及ExecutorServerApi
        String executorAddr = tableCoordinatorServerApi.getExecutorAddr(tableName);
        Map<String, String> allIndexExecutorAddr = tableCoordinatorServerApi.getAllIndexExecutorAddr(tableName);

        ExecutorServerApi executorServerApi = null;
        Map<String, ExecutorServerApi> allIndexExecutorServerApi = new HashMap<>();

        // 6. 插入未完成主键数据
        executorServerApi.insertUnfinishedRecord(keyValue);

        // 7. 插入所有索引数据
        for (String indexName : allIndexExecutorServerApi.keySet()) {
            allIndexExecutorServerApi.get(indexName).insertIndex(allIndexKeyValue.get(indexName));
        }

        // 8. 插入完成主键数据
        executorServerApi.insertFinishedRecord(keyValue.getKey(), tableDefinition.getVersion());
    }

    public void executeUpdate(String tableName, Object[] record) throws Exception {
        // 1. 获取该Table对应的tableCoordinator服务地址
        String tableCoordinatorAddr = coordinatorServerApi.getTableCoordinatorAddr(tableName);
        // 2. 使用tableCoordinatorAddr初始化tableCoordinatorServerApi，优先使用缓存
        if (!tableCoordinatorServerApiMap.containsKey(tableName)) {
            TableCoordinatorServerApi tableCoordinatorServerApi = null;
            tableCoordinatorServerApiMap.put(tableName, tableCoordinatorServerApi);
        }
        TableCoordinatorServerApi tableCoordinatorServerApi = tableCoordinatorServerApiMap.get(tableName);
        // 3. 获取TableDefinition
        TableDefinition tableDefinition = tableCoordinatorServerApi.getTableDefinition(tableName);

        // 4. 获取旧数据
        String executorAddr = tableCoordinatorServerApi.getExecutorAddr(tableName);
        ExecutorServerApi executorServerApi = null;
        KeyValueCodec keyValueCodec = null;
        KeyValue oldKeyValue = executorServerApi.getRecord(keyValueCodec.encodeKey(record));
        Object[] oldRecord = keyValueCodec.decode(oldKeyValue);

        // 5. 获取有变化的列 及 对应的索引
        int[] changedColumnIndex = null;
        List<String> changedIndexName = null;



        // 6. 序列化新旧索引数据
        Map<String, KeyValueCodec> changedIndexKeyValueCodec = null;
        Map<String, KeyValue> oldIndexKeyValue = new HashMap<>();
        for (String indexName : changedIndexKeyValueCodec.keySet()) {
            oldIndexKeyValue.put(indexName, changedIndexKeyValueCodec.get(indexName).encode(record));
        }

        Map<String, KeyValueCodec> newIndexKeyValueCodec = null;
        KeyValue newKeyValue = keyValueCodec.encode(oldRecord);
        Map<String, KeyValue> newIndexKeyValue = new HashMap<>();
        for (String indexName : changedIndexKeyValueCodec.keySet()) {
            newIndexKeyValue.put(indexName, changedIndexKeyValueCodec.get(indexName).encode(record));
        }

        // 5. 获取变化索引地址及ExecutorServerApi
        Map<String, String> changedIndexExecutorAddr = tableCoordinatorServerApi.getIndexExecutorAddr(tableName, changedIndexName);
        Map<String, ExecutorServerApi> changedIndexExecutorServerApi = new HashMap<>();

        // 6. 插入未完成主键数据
        executorServerApi.insertUnfinishedRecord(newKeyValue);

        // 7. 插入所有索引数据
        for (String indexName : changedIndexExecutorServerApi.keySet()) {
            changedIndexExecutorServerApi.get(indexName).insertIndex(newIndexKeyValue.get(indexName));
        }

        // 8. 删除旧索引数据
        for (String indexName : changedIndexExecutorServerApi.keySet()) {
            changedIndexExecutorServerApi.get(indexName).deleteIndex(oldIndexKeyValue.get(indexName));
        }

        // 9. 删除旧完成主键
        executorServerApi.deleteFinishedRecord(oldKeyValue.getKey());

        // 10. 插入完成主键数据
        executorServerApi.insertFinishedRecord(newKeyValue.getKey(), tableDefinition.getVersion());
    }

    public void executeDelete(String tableName, Object[] record) throws Exception {
        // 1. 获取该Table对应的tableCoordinator服务地址
        String tableCoordinatorAddr = coordinatorServerApi.getTableCoordinatorAddr(tableName);
        // 2. 使用tableCoordinatorAddr初始化tableCoordinatorServerApi，优先使用缓存
        if (!tableCoordinatorServerApiMap.containsKey(tableName)) {
            TableCoordinatorServerApi tableCoordinatorServerApi = null;
            tableCoordinatorServerApiMap.put(tableName, tableCoordinatorServerApi);
        }
        TableCoordinatorServerApi tableCoordinatorServerApi = tableCoordinatorServerApiMap.get(tableName);
        // 3. 获取TableDefinition
        TableDefinition tableDefinition = tableCoordinatorServerApi.getTableDefinition(tableName);
        // 4. 序列化主键索引及其他索引数据
        KeyValueCodec keyValueCodec = null;
        Map<String, KeyValueCodec> allIndexKeyValueCodec = null;
        KeyValue keyValue = keyValueCodec.encode(record);
        Map<String, KeyValue> allIndexKeyValue = new HashMap<>();
        for (String indexName : allIndexKeyValueCodec.keySet()) {
            allIndexKeyValue.put(indexName, allIndexKeyValueCodec.get(indexName).encode(record));
        }

        // 5. 获取所有索引包含主键的executor地址及ExecutorServerApi
        String executorAddr = tableCoordinatorServerApi.getExecutorAddr(tableName);
        Map<String, String> allIndexExecutorAddr = tableCoordinatorServerApi.getAllIndexExecutorAddr(tableName);

        ExecutorServerApi executorServerApi = null;
        Map<String, ExecutorServerApi> allIndexExecutorServerApi = new HashMap<>();

        // 6. 插入删除主键数据
        executorServerApi.insertDeleteKey(keyValue);

        // 7. 删除所有索引数据
        for (String indexName : allIndexExecutorServerApi.keySet()) {
            allIndexExecutorServerApi.get(indexName).deleteIndex(allIndexKeyValue.get(indexName));
        }

        // 8. 删除完成主键数据
        executorServerApi.deleteFinishedRecord(keyValue.getKey());

        // 9. 删除标记为删除的主键
        executorServerApi.deleteDeleteKey(keyValue.getKey());
    }


    public void executeInsert2(String tableName, Object[] record) throws Exception {
        CommonId id = metaServiceApi.getTableId(null, tableName);
        TableDefinition tableDefinition = metaServiceApi.getTableDefinition(id, tableName);
        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions =
            metaServiceApi.getParts(id, tableName);
        KeyValueCodec keyValueCodec = new DingoKeyValueCodec(
            tableDefinition.getDingoType(),
            tableDefinition.getKeyMapping()
        );
        RangeStrategy rangeStrategy = new RangeStrategy(tableDefinition, partitions.navigableKeySet());
        RouteTable routeTable = new RouteTable(
            tableName,
            id,
            keyValueCodec,
            partitions,
            rangeStrategy);
        Map<String, RouteTable> dingoRouteTables = new HashMap<>();
        dingoRouteTables.put(tableName, routeTable);

        KeyValue pk = keyValueCodec.encode(record);

        ExecutorApi executorApi = routeTable.getLeaderAddress(apiRegistry, routeTable.getStartPartitionKey(apiRegistry, pk.getKey()));

        executorApi.insertUnfinishedRecord(id, pk);

        for (Map.Entry<String, Index> indexEntry : tableDefinition.getIndexes().entrySet()) {
            CommonId indexId = metaServiceApi.getTableId(null, tableName + "_" + indexEntry.getKey());
            TableDefinition indexTableDefinition = metaServiceApi.getTableDefinition(indexId, tableName + "_" + indexEntry.getKey());

            List<ColumnDefinition> columns = indexTableDefinition.getColumns();
            Object[] indexRecord = new Object[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                indexRecord[i] = record[tableDefinition.getColumnIndex(columns.get(i).getName())];
            }
            KeyValueCodec indexCodec = new DingoKeyValueCodec(indexTableDefinition.getDingoType(), indexTableDefinition.getKeyMapping());
            KeyValue indexKeyValue = indexCodec.encode(indexRecord);
            executorApi.upsertKeyValue(indexId, indexKeyValue);
        }

        executorApi.insertFinishedRecord(id, pk.getKey(), tableDefinition.getVersion());
        executorApi.deleteUnfinishedRecord(id, pk.getKey());
    }
}
