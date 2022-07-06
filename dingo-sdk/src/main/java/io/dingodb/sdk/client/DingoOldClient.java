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
import io.dingodb.sdk.common.Column;
import io.dingodb.sdk.common.Key;
import io.dingodb.server.api.ExecutorApi;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

@Slf4j
public class DingoOldClient extends ClientBase {
    private MetaClient metaClient;

    private ApiRegistry apiRegistry;

    /**
     * the parameters below should be moved to a new class.
     * as the connection should dependent from the table and database.
     * // todo Huzx
     */
    private CommonId tableId;
    private KeyValueCodec codec;
    private NavigableMap<ByteArrayUtils.ComparableByteArray, Part> parts;
    private NavigableMap<ByteArrayUtils.ComparableByteArray, ExecutorApi> partsApi;
    private PartitionStrategy<ByteArrayUtils.ComparableByteArray> ps;

    private final String tableName;

    private Integer retryTime = 3;


    /*
    * add constructor to support retry, using connection string, current host and current port.
    * // todo Huzx
    */
    public DingoOldClient(String configPath, String tableName) throws Exception {
        this(configPath, tableName, 0);
    }

    public DingoOldClient(String configPath, String tableName, Integer retryTime) throws Exception {
        super(configPath);
        this.tableName = tableName;
        this.metaClient = new MetaClient(configPath);
        this.apiRegistry = super.getNetService().apiRegistry();
        setRetryTime(retryTime);
        refreshTableMeta();
    }

    public DingoOldClient(String coordinatorExchangeSvrList, String currentHost, Integer currentPort, String tableName) {
        this(coordinatorExchangeSvrList, currentHost, currentPort, tableName, 0);
    }

    public DingoOldClient(String coordinatorExchangeSvrList, String currentHost,
                          Integer currentPort, String tableName, Integer retryTime) {
        super(coordinatorExchangeSvrList, currentHost, currentPort);
        this.tableName = tableName;
        this.metaClient = new MetaClient(coordinatorExchangeSvrList, currentHost, currentPort);
        this.apiRegistry = super.getNetService().apiRegistry();
        setRetryTime(retryTime);
        refreshTableMeta();
    }

    public boolean put(Key key, Column[] columns) throws Exception {
        return put(key, columns);
    }

    public boolean insert(Object[] record) throws Exception {
        int retryTime = 0;
        Exception exception = null;
        do {
            try {
                KeyValue keyValue = codec.encode(record);
                ExecutorApi executorApi = getExecutor(ps.calcPartId(keyValue.getKey()));
                return internalInsert(executorApi, keyValue);
            } catch (Exception e) {
                exception = e;
                log.error("insert record {} failed, tableId:{} retryTime:{} ",
                    Arrays.toString(record), tableId, retryTime, e);
                refreshTableMeta();
            } finally {
                retryTime++;
            }
        }
        while (retryTime <= this.retryTime);
        throw exception;
    }

    public boolean insert(List<Object[]> records) throws Exception {
        return insert(records, 1000);
    }

    public boolean insert(List<Object[]> records, Integer batchSize) throws Exception {
        int retryTime = 0;
        Exception exception = null;
        do {
            Map<ByteArrayUtils.ComparableByteArray, List<KeyValue>> recordGroup
                = new HashMap<ByteArrayUtils.ComparableByteArray, List<KeyValue>>();
            try {
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
            } catch (Exception e) {
                exception = e;
                log.error("batch insert record failed, tableId:{} retryTime:{} ",
                    tableId, retryTime, e);
                refreshTableMeta();
            } finally {
                retryTime++;
            }
        }
        while (retryTime <= this.retryTime);
        throw exception;
    }


    private boolean internalInsert(ExecutorApi executorApi, KeyValue keyValue) throws Exception {
        try {
            return executorApi.upsertKeyValue(tableId, keyValue);
        } catch (Exception e) {
            log.error("insert keyValue record failed, tableId:{} retryTime:{} ", tableId, retryTime, e);
            throw e;
        }
    }

    private boolean internalInsert(ExecutorApi executorApi, List<KeyValue> keyValues) throws Exception {
        try {
            return executorApi.upsertKeyValue(tableId, keyValues);
        } catch (Exception e) {
            log.error("insert KeyValue error, tableId:{} retryTime:{} ", tableId,  retryTime, e);
            throw e;
        }
    }

    public List<Object[]> get(Object[] startKey, Object[] endKey) throws Exception {
        int retryTime = 0;
        Exception exception = null;
        do {
            try {
                byte[] bytesStartKey = codec.encodeKey(startKey);
                byte[] bytesEndKey = codec.encodeKey(endKey);
                ExecutorApi executorApi = getExecutor(ps.calcPartId(bytesStartKey));
                List<KeyValue> keyValues = executorApi.getKeyValueByRange(tableId, bytesStartKey, bytesEndKey);
                List<Object[]> result = new ArrayList<Object[]>();
                for (KeyValue kv: keyValues) {
                    result.add(codec.decode(kv));
                }
                return result;
            } catch (Exception e) {
                exception = e;
                log.error("get key value by range {}:{} catch error,tableId:{} retryTime:{} ",
                    Arrays.toString(startKey), Arrays.toString(endKey), tableId,  retryTime, e);
                refreshTableMeta();
            } finally {
                retryTime++;
            }
        }
        while (retryTime <= this.retryTime);
        throw exception;
    }

    public Object[] get(Object[] key) throws Exception {
        int retryTime = 0;
        Exception exception = null;
        do {
            try {
                byte[] primaryKey = codec.encodeKey(key);
                ExecutorApi executorApi = getExecutor(ps.calcPartId(primaryKey));
                return codec.mapKeyAndDecodeValue(key, executorApi.getValueByPrimaryKey(tableId, primaryKey));
            } catch (Exception e) {
                exception = e;
                log.error("get key value by key {} catch error,tableId:{} retryTime:{} ",
                    Arrays.toString(key), tableId,  retryTime, e);
                refreshTableMeta();
            } finally {
                retryTime++;
            }
        }
        while (retryTime <= this.retryTime);
        throw exception;
    }

    public boolean delete(Object[] key) throws Exception {
        int retryTime = 0;
        Exception exception = null;
        do {
            try {
                byte[] primaryKey = codec.encodeKey(key);
                ExecutorApi executorApi = getExecutor(ps.calcPartId(primaryKey));
                return executorApi.delete(tableId, primaryKey);
            } catch (Exception e) {
                exception = e;
                log.error("delete key {} catch error,tableId:{} retryTime:{} ",
                    Arrays.toString(key), tableId,  retryTime, e);
                refreshTableMeta();
            } finally {
                retryTime++;
            }
        }
        while (retryTime <= this.retryTime);
        throw exception;
    }

    public void refreshTableMeta() {
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
