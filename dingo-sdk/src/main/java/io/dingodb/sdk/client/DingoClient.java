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

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.PartitionStrategy;
import io.dingodb.common.partition.RangeStrategy;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.KeyValueCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.Part;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.ExecutorApi;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

@Slf4j
public class DingoClient extends ClientBase {
    private MetaClient metaClient;

    private ApiRegistry apiRegistry;
    private CommonId tableId;
    private KeyValueCodec codec;
    private NavigableMap<ByteArrayUtils.ComparableByteArray, Part> parts;
    private NavigableMap<ByteArrayUtils.ComparableByteArray, ExecutorApi> partsApi;
    private PartitionStrategy<ByteArrayUtils.ComparableByteArray> ps;

    private final String tableName;

    private Integer retryTime;

    public DingoClient(String configPath, String tableName) throws Exception {
        this(configPath, tableName, 0);
    }

    public DingoClient(String configPath, String tableName, Integer retryTime) throws Exception {
        super(configPath);
        this.tableName = tableName;
        this.metaClient = new MetaClient(configPath);
        this.apiRegistry = super.getNetService().apiRegistry();
        setRetryTime(retryTime);
        refreshTableMeta();
    }

    public DingoClient(String coordinatorExchangeSvrList, String currentHost, Integer currentPort, String tableName) {
        this(coordinatorExchangeSvrList, currentHost, currentPort, tableName, 0);
    }

    public DingoClient(String coordinatorExchangeSvrList, String currentHost,
                       Integer currentPort, String tableName, Integer retryTime) {
        super(coordinatorExchangeSvrList, currentHost, currentPort);
        this.tableName = tableName;
        this.metaClient = new MetaClient(coordinatorExchangeSvrList, currentHost, currentPort);
        this.apiRegistry = super.getNetService().apiRegistry();
        setRetryTime(retryTime);
        refreshTableMeta();
    }


    public boolean insert(Object[] record) throws Exception {
        KeyValue keyValue = codec.encode(record);
        ExecutorApi executorApi = getExecutor(ps.calcPartId(keyValue.getKey()));
        return internalInsert(executorApi, keyValue);
    }

    public boolean insert(List<Object[]> records) throws Exception {
        return insert(records, 1000);
    }

    public boolean insert(List<Object[]> records, Integer batchSize) throws Exception {
        Map<ByteArrayUtils.ComparableByteArray, List<KeyValue>> recordGroup
            = new HashMap<ByteArrayUtils.ComparableByteArray, List<KeyValue>>();
        for (Object[] record : records) {
            KeyValue keyValue = codec.encode(record);
            ByteArrayUtils.ComparableByteArray keyId = ps.calcPartId(keyValue.getKey());
            List<KeyValue> currentGroup;
            currentGroup = recordGroup.get(keyId);
            if (currentGroup == null) {
                currentGroup = new ArrayList<KeyValue>();
                recordGroup.put(keyId, currentGroup);
            }
            currentGroup.add(keyValue);
            if (currentGroup.size() >= batchSize) {
                internalInsert(getExecutor(keyId), currentGroup);
                currentGroup.clear();
            }
        }
        for (Map.Entry<ByteArrayUtils.ComparableByteArray, List<KeyValue>> entry : recordGroup.entrySet()) {
            if (entry.getValue().size() > 0) {
                internalInsert(getExecutor(entry.getKey()), entry.getValue());
            }
        }
        return true;
    }


    private boolean internalInsert(ExecutorApi executorApi, KeyValue keyValue) throws Exception {
        int retryTime = 0;
        Exception exception = null;
        do {
            try {
                return executorApi.upsertKeyValue(tableId, keyValue);
            } catch (Exception e) {
                exception = e;
                log.error("insert keyValue record failed, tableId:{} retryTime:{} ", tableId, retryTime, e);
                refreshTableMeta();
            } finally {
                retryTime++;
            }
        }
        while (retryTime <= this.retryTime);
        throw exception;
    }

    private boolean internalInsert(ExecutorApi executorApi, List<KeyValue> keyValues) throws Exception {
        int retryTime = 0;
        Exception exception = null;
        do {
            try {
                return executorApi.upsertKeyValue(tableId, keyValues);
            } catch (Exception e) {
                exception = e;
                refreshTableMeta();
                log.error("insert KeyValue error,tableId:{} retryTime:{} ", tableId,  retryTime, e);
            } finally {
                retryTime++;
            }
        }
        while (retryTime <= this.retryTime);
        throw exception;
    }

    public List<Object[]> get(Object[] startKey, Object[] endKey) throws Exception {
        byte[] bytesStartKey = codec.encodeKey(startKey);
        byte[] bytesEndKey = codec.encodeKey(endKey);
        ExecutorApi executorApi = getExecutor(ps.calcPartId(bytesStartKey));
        int retryTime = 0;
        Exception exception = null;
        do {
            try {
                List<KeyValue> keyValues = executorApi.getKeyValueByRange(tableId, bytesStartKey, bytesEndKey);
                List<Object[]> result = new ArrayList<Object[]>();
                for (KeyValue kv: keyValues) {
                    result.add(codec.decode(kv));
                }
                return result;
            } catch (Exception e) {
                exception = e;
                log.error("get key value by range catch error,tableId:{} retryTime:{} ", tableId,  retryTime, e);
                refreshTableMeta();
            } finally {
                retryTime++;
            }
        }
        while (retryTime <= this.retryTime);
        throw exception;
    }

    public Object[] get(Object[] key) throws Exception {
        byte[] primaryKey = codec.encodeKey(key);
        ExecutorApi executorApi = getExecutor(ps.calcPartId(primaryKey));
        int retryTime = 0;
        Exception exception = null;
        do {
            try {
                return codec.mapKeyAndDecodeValue(key, executorApi.getValueByPrimaryKey(tableId, primaryKey));
            } catch (Exception e) {
                exception = e;
                log.error("get key value by key catch error,tableId:{} retryTime:{} ", tableId,  retryTime, e);
                refreshTableMeta();
            } finally {
                retryTime++;
            }
        }
        while (retryTime <= this.retryTime);
        throw exception;
    }

    public boolean delete(Object[] key) throws Exception {
        byte[] primaryKey = codec.encodeKey(key);
        ExecutorApi executorApi = getExecutor(ps.calcPartId(primaryKey));
        int retryTime = 0;
        Exception exception = null;
        do {
            try {
                return executorApi.delete(tableId, primaryKey);
            } catch (Exception e) {
                exception = e;
                log.error("delete key catch error,tableId:{} retryTime:{} ", tableId,  retryTime, e);
                refreshTableMeta();
            } finally {
                retryTime++;
            }
        }
        while (retryTime <= this.retryTime);
        throw exception;
    }

    private void refreshTableMeta() {
        this.tableId = metaClient.getTableId(tableName);
        TableDefinition tableDefinition = metaClient.getTableDefinition(tableName);
        if (tableDefinition == null) {
            System.out.printf("Table:%s not found \n", tableName);
            System.exit(1);
        }
        this.parts = metaClient.getParts(tableName);
        this.codec = new KeyValueCodec(tableDefinition.getTupleSchema(), tableDefinition.getKeyMapping());
        this.ps = new RangeStrategy(tableDefinition, parts.navigableKeySet());
        this.partsApi = new TreeMap<ByteArrayUtils.ComparableByteArray, ExecutorApi>();
    }

    private ExecutorApi getExecutor(ByteArrayUtils.ComparableByteArray byteArray) {
        ExecutorApi executorApi = partsApi.get(byteArray);
        if (executorApi != null) {
            return executorApi;
        }
        Part part = parts.get(byteArray);
        ExecutorApi executor = apiRegistry
            .proxy(ExecutorApi.class, () -> new Location(part.getLeader().getHost(), part.getLeader().getPort()));
        partsApi.put(byteArray, executor);
        return executor;
    }

    public void setRetryTime(Integer retryTime) {
        if (retryTime >= 0) {
            this.retryTime = retryTime;
        } else {
            this.retryTime = 0;
        }
    }

    public MetaClient getMetaClient() {
        return metaClient;
    }
}
