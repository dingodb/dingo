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

package io.dingodb.server.executor.index;

import io.dingodb.common.CommonId;
import io.dingodb.common.codec.DingoIndexKeyValueCodec;
import io.dingodb.common.codec.DingoKeyValueCodec;
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.partition.RangeStrategy;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.Part;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.ExecutorApi;
import io.dingodb.server.api.TableApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.client.connector.impl.ServiceConnector;
import io.dingodb.server.client.meta.service.MetaServiceClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class DingoIndexDataExecutor {

    MetaServiceClient metaServiceClient;

    private CommonId tableId;

    public DingoIndexDataExecutor(CoordinatorConnector coordinatorConnector, CommonId tableId) {
        metaServiceClient = new MetaServiceClient(coordinatorConnector);
        this.tableId = tableId;
    }

    public void executeInsert(Object[] record) throws Exception {
        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableId);

        int tableDefinitionVersion = tableDefinition.getVersion();

        ServiceConnector serviceConnector = metaServiceClient.getTableConnector(tableId);
        TableApi tableApi = ApiRegistry.getDefault().proxy(TableApi.class, serviceConnector);

        KeyValueCodec codec = new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
        KeyValue keyValue = codec.encode(record);

        byte[] unfinishedKey = new byte[keyValue.getKey().length+1];
        byte[] finishedKey = new byte[keyValue.getKey().length+1];
        unfinishedKey[0] = 0;
        finishedKey[0] = 1;
        System.arraycopy(keyValue.getKey(), 0, unfinishedKey, 1, keyValue.getKey().length);
        System.arraycopy(keyValue.getKey(), 0, finishedKey, 1, keyValue.getKey().length);

        KeyValue unfinishedKeyValue = new KeyValue(unfinishedKey, keyValue.getValue());
        KeyValue finishedKeyValue = new KeyValue(finishedKey, keyValue.getValue());

        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions =
            metaServiceClient.getParts(tableId);

        RangeStrategy rangeStrategy = new RangeStrategy(tableDefinition, partitions.navigableKeySet());

        ByteArrayUtils.ComparableByteArray unfinishedPartId = rangeStrategy.calcPartId(unfinishedKey);
        ServiceConnector unfinishedPartConnector = new ServiceConnector(tableId, partitions.get(unfinishedPartId).getReplicates());
        ExecutorApi unfinishedExecutorcApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, unfinishedPartConnector);

        ByteArrayUtils.ComparableByteArray finishedPartId = rangeStrategy.calcPartId(finishedKey);
        ServiceConnector finishedPartConnector = new ServiceConnector(tableId, partitions.get(finishedPartId).getReplicates());
        ExecutorApi finishedExecutorcApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, finishedPartConnector);

        boolean b1 = unfinishedExecutorcApi.upsertKeyValue(null, null, tableId, unfinishedKeyValue);

        Map<String, DingoIndexKeyValueCodec> indexCodecs = getDingoIndexCodec(tableDefinition);
        for (String indexName : indexCodecs.keySet()) {
            DingoIndexKeyValueCodec indexCodec = indexCodecs.get(indexName);
            KeyValue indexKeyValue = indexCodec.encode(record);
            CommonId indexId = tableApi.getIndexId(tableId, indexName);
            ServiceConnector indexServiceConnector = new ServiceConnector(indexId, serviceConnector.getAddresses());
            ExecutorApi executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, indexServiceConnector);
            boolean b = executorApi.upsertKeyValue(null, null, indexId, indexKeyValue);
        }

        TableDefinition currentTd = metaServiceClient.getTableDefinition(tableId);

        if (currentTd.getVersion() != tableDefinitionVersion) {
            throw new Exception("table definition changed");
        }

        boolean b2 = finishedExecutorcApi.upsertKeyValue(null, null, tableId, finishedKeyValue);
        boolean b3 = unfinishedExecutorcApi.delete(null, null, tableId, unfinishedKeyValue.getPrimaryKey());
    }

    public void executeUpdate(Object[] record) throws Exception {
        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableId);

        int tableDefinitionVersion = tableDefinition.getVersion();

        ServiceConnector serviceConnector = metaServiceClient.getTableConnector(tableId);
        TableApi tableApi = ApiRegistry.getDefault().proxy(TableApi.class, serviceConnector);

        KeyValueCodec codec = new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
        KeyValue keyValue = codec.encode(record);

        byte[] unfinishedKey = new byte[keyValue.getKey().length+1];
        byte[] finishedKey = new byte[keyValue.getKey().length+1];
        byte[] deleteKey = new byte[keyValue.getKey().length+1];
        unfinishedKey[0] = 0;
        finishedKey[0] = 1;
        deleteKey[0] = 2;
        System.arraycopy(keyValue.getKey(), 0, unfinishedKey, 1, keyValue.getKey().length);
        System.arraycopy(keyValue.getKey(), 0, finishedKey, 1, keyValue.getKey().length);
        System.arraycopy(keyValue.getKey(), 0, deleteKey, 1, keyValue.getKey().length);

        KeyValue unfinishedKeyValue = new KeyValue(unfinishedKey, keyValue.getValue());
        KeyValue finishedKeyValue = new KeyValue(finishedKey, keyValue.getValue());

        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions =
            metaServiceClient.getParts(tableId);

        RangeStrategy rangeStrategy = new RangeStrategy(tableDefinition, partitions.navigableKeySet());

        ByteArrayUtils.ComparableByteArray unfinishedPartId = rangeStrategy.calcPartId(unfinishedKey);
        ServiceConnector unfinishedPartConnector = new ServiceConnector(tableId, partitions.get(unfinishedPartId).getReplicates());
        ExecutorApi unfinishedExecutorcApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, unfinishedPartConnector);

        ByteArrayUtils.ComparableByteArray finishedPartId = rangeStrategy.calcPartId(finishedKey);
        ServiceConnector finishedPartConnector = new ServiceConnector(tableId, partitions.get(finishedPartId).getReplicates());
        ExecutorApi finishedExecutorcApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, finishedPartConnector);

        boolean b1 = unfinishedExecutorcApi.upsertKeyValue(null, null, tableId, unfinishedKeyValue);

        byte[] oldValue = finishedExecutorcApi.getValueByPrimaryKey(null, null, tableId, finishedKeyValue.getPrimaryKey());
        KeyValue oldKeyValue = new KeyValue(keyValue.getKey(), oldValue);
        Object[] oldRecord = codec.decode(oldKeyValue);

        Map<String, DingoIndexKeyValueCodec> indexCodecs = getDingoIndexCodec(tableDefinition);
        for (String indexName : indexCodecs.keySet()) {
            DingoIndexKeyValueCodec indexCodec = indexCodecs.get(indexName);
            KeyValue indexKeyValue = indexCodec.encode(oldRecord);
            CommonId indexId = tableApi.getIndexId(tableId, indexName);
            ServiceConnector indexServiceConnector = new ServiceConnector(indexId, serviceConnector.getAddresses());
            ExecutorApi executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, indexServiceConnector);
            boolean b2 = executorApi.delete(null, null, indexId, indexKeyValue.getPrimaryKey());
        }

        boolean b3 = finishedExecutorcApi.delete(null, null, tableId, finishedKeyValue.getPrimaryKey());

        for (String indexName : indexCodecs.keySet()) {
            DingoIndexKeyValueCodec indexCodec = indexCodecs.get(indexName);
            KeyValue indexKeyValue = indexCodec.encode(record);
            CommonId indexId = tableApi.getIndexId(tableId, indexName);
            ServiceConnector indexServiceConnector = new ServiceConnector(indexId, serviceConnector.getAddresses());
            ExecutorApi executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, indexServiceConnector);
            boolean b4 = executorApi.upsertKeyValue(null, null, indexId, indexKeyValue);
        }

        TableDefinition currentTd = metaServiceClient.getTableDefinition(tableId);

        if (currentTd.getVersion() != tableDefinitionVersion) {
            throw new Exception("table definition changed");
        }

        boolean b5 = finishedExecutorcApi.upsertKeyValue(null, null, tableId, finishedKeyValue);
        boolean b6 = unfinishedExecutorcApi.delete(null, null, tableId, unfinishedKeyValue.getPrimaryKey());
    }

    public void executeDelete(Object[] record) throws Exception {
        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableId);

        int tableDefinitionVersion = tableDefinition.getVersion();

        ServiceConnector serviceConnector = metaServiceClient.getTableConnector(tableId);
        TableApi tableApi = ApiRegistry.getDefault().proxy(TableApi.class, serviceConnector);

        KeyValueCodec codec = new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
        KeyValue keyValue = codec.encode(record);

        byte[] finishedKey = new byte[keyValue.getKey().length+1];
        byte[] deleteKey = new byte[keyValue.getKey().length+1];
        finishedKey[0] = 1;
        deleteKey[0] = 2;
        System.arraycopy(keyValue.getKey(), 0, finishedKey, 1, keyValue.getKey().length);
        System.arraycopy(keyValue.getKey(), 0, deleteKey, 1, keyValue.getKey().length);

        KeyValue finishedKeyValue = new KeyValue(finishedKey, keyValue.getValue());
        KeyValue deleteKeyValue = new KeyValue(deleteKey, keyValue.getValue());

        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions =
            metaServiceClient.getParts(tableId);

        RangeStrategy rangeStrategy = new RangeStrategy(tableDefinition, partitions.navigableKeySet());

        ByteArrayUtils.ComparableByteArray finishedPartId = rangeStrategy.calcPartId(finishedKey);
        ServiceConnector finishedPartConnector = new ServiceConnector(tableId, partitions.get(finishedPartId).getReplicates());
        ExecutorApi finishedExecutorcApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, finishedPartConnector);

        ByteArrayUtils.ComparableByteArray deletePartId = rangeStrategy.calcPartId(deleteKey);
        ServiceConnector deletePartConnector = new ServiceConnector(tableId, partitions.get(deletePartId).getReplicates());
        ExecutorApi deleteExecutorcApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, deletePartConnector);


        boolean b1 = deleteExecutorcApi.upsertKeyValue(null, null, tableId, deleteKeyValue);

        Map<String, DingoIndexKeyValueCodec> indexCodecs = getDingoIndexCodec(tableDefinition);
        for (String indexName : indexCodecs.keySet()) {
            DingoIndexKeyValueCodec indexCodec = indexCodecs.get(indexName);
            KeyValue indexKeyValue = indexCodec.encode(record);
            CommonId indexId = tableApi.getIndexId(tableId, indexName);
            ServiceConnector indexServiceConnector = new ServiceConnector(indexId, serviceConnector.getAddresses());
            ExecutorApi executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, indexServiceConnector);
            boolean b = executorApi.delete(null, null, indexId, indexKeyValue.getPrimaryKey());
        }

        TableDefinition currentTd = metaServiceClient.getTableDefinition(tableId);

        if (currentTd.getVersion() != tableDefinitionVersion) {
            throw new Exception("table definition changed");
        }

        boolean b2 = finishedExecutorcApi.delete(null, null, tableId, finishedKeyValue.getPrimaryKey());
        boolean b3 = deleteExecutorcApi.delete(null, null, tableId, deleteKeyValue.getPrimaryKey());
    }

    public void executeInsertIndex(Object[] record, String indexName) throws Exception {
        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableId);

        int tableDefinitionVersion = tableDefinition.getVersion();

        ServiceConnector serviceConnector = metaServiceClient.getTableConnector(tableId);
        TableApi tableApi = ApiRegistry.getDefault().proxy(TableApi.class, serviceConnector);

        KeyValueCodec codec = new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
        KeyValue keyValue = codec.encode(record);

        byte[] unfinishedKey = new byte[keyValue.getKey().length+1];
        byte[] finishedKey = new byte[keyValue.getKey().length+1];
        unfinishedKey[0] = 0;
        finishedKey[0] = 1;
        System.arraycopy(keyValue.getKey(), 0, unfinishedKey, 1, keyValue.getKey().length);
        System.arraycopy(keyValue.getKey(), 0, finishedKey, 1, keyValue.getKey().length);

        KeyValue unfinishedKeyValue = new KeyValue(unfinishedKey, keyValue.getValue());
        KeyValue finishedKeyValue = new KeyValue(finishedKey, keyValue.getValue());

        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions =
            metaServiceClient.getParts(tableId);

        RangeStrategy rangeStrategy = new RangeStrategy(tableDefinition, partitions.navigableKeySet());

        ByteArrayUtils.ComparableByteArray unfinishedPartId = rangeStrategy.calcPartId(unfinishedKey);
        ServiceConnector unfinishedPartConnector = new ServiceConnector(tableId, partitions.get(unfinishedPartId).getReplicates());
        ExecutorApi unfinishedExecutorcApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, unfinishedPartConnector);

        ByteArrayUtils.ComparableByteArray finishedPartId = rangeStrategy.calcPartId(finishedKey);
        ServiceConnector finishedPartConnector = new ServiceConnector(tableId, partitions.get(finishedPartId).getReplicates());
        ExecutorApi finishedExecutorcApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, finishedPartConnector);

        Map<String, DingoIndexKeyValueCodec> indexCodecs = getDingoIndexCodec(tableDefinition);

        DingoIndexKeyValueCodec indexCodec = indexCodecs.get(indexName);
        KeyValue IndexKeyValue = indexCodec.encode(record);
        CommonId indexId = tableApi.getIndexId(tableId, indexName);
        ServiceConnector indexServiceConnector = new ServiceConnector(indexId, serviceConnector.getAddresses());
        ExecutorApi executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, indexServiceConnector);
        boolean b1 = executorApi.upsertKeyValue(null, null, indexId, IndexKeyValue);

        TableDefinition currentTd = metaServiceClient.getTableDefinition(tableId);

        if (currentTd.getVersion() != tableDefinitionVersion) {
            throw new Exception("table definition changed");
        }

        boolean b2 = finishedExecutorcApi.upsertKeyValue(null, null, tableId, finishedKeyValue);
        boolean b3 = unfinishedExecutorcApi.delete(null, null, tableId, unfinishedKeyValue.getPrimaryKey());
    }

    public List<byte[]> getKeyByteByIndex(String indexName, Object[] record) throws Exception {
        ServiceConnector serviceConnector = metaServiceClient.getTableConnector(tableId);
        TableApi tableApi = ApiRegistry.getDefault().proxy(TableApi.class, serviceConnector);
        CommonId indexId = tableApi.getIndexId(tableId, indexName);
        ServiceConnector indexServiceConnector = new ServiceConnector(indexId, serviceConnector.getAddresses());
        ExecutorApi executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, indexServiceConnector);

        TableDefinition tableDefinition = tableApi.getDefinition(tableId);
        DingoIndexKeyValueCodec indexCodec = getDingoIndexCodec(tableDefinition).get(indexName);
        byte[] indexKey = indexCodec.encodeIndexKey(record);
        List<KeyValue> keyValues = executorApi.getKeyValueByKeyPrefix(null, null, indexId, indexKey);

        List<byte[]> keys = new ArrayList<>();
        for(KeyValue keyValue : keyValues) {
            keys.add(indexCodec.decodeKeyBytes(keyValue));
        }
        return keys;
    }

    public List<Object[]> getRecordsByIndex(String indexName, Object[] record) throws Exception {
        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableId);
        ServiceConnector serviceConnector = metaServiceClient.getTableConnector(tableId);
        TableApi tableApi = ApiRegistry.getDefault().proxy(TableApi.class, serviceConnector);
        CommonId indexId = tableApi.getIndexId(tableId, indexName);
        ServiceConnector indexServiceConnector = new ServiceConnector(indexId, serviceConnector.getAddresses());
        ExecutorApi executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, indexServiceConnector);
        DingoIndexKeyValueCodec indexCodec = getDingoIndexCodec(tableDefinition).get(indexName);
        byte[] indexKey = indexCodec.encodeIndexKey(record);
        List<KeyValue> keyValues = executorApi.getKeyValueByKeyPrefix(null, null, indexId, indexKey);

        DingoKeyValueCodec codec = new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
        List<Object[]> records = new ArrayList<>();
        for(KeyValue keyValue : keyValues) {
            byte[] key = indexCodec.decodeKeyBytes(keyValue);
            byte[] finishedKey = new byte[key.length+1];
            finishedKey[0] = 1;
            System.arraycopy(key, 0, finishedKey, 1, key.length);
            NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions =
                metaServiceClient.getParts(tableId);
            RangeStrategy rangeStrategy = new RangeStrategy(tableDefinition, partitions.navigableKeySet());
            ByteArrayUtils.ComparableByteArray partId = rangeStrategy.calcPartId(finishedKey);
            ServiceConnector finishedPartConnector = new ServiceConnector(tableId, partitions.get(partId).getReplicates());
            ExecutorApi tableExecutorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, finishedPartConnector);
            byte[] value = tableExecutorApi.getValueByPrimaryKey(null, null, tableId, finishedKey);
            KeyValue oriKeyValue = new KeyValue(key, value);
            records.add(codec.decode(oriKeyValue));
        }
        return records;
    }

    public List<Object[]> getUnfinishedRecord() throws Exception {
        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableId);
        DingoKeyValueCodec codec = new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions = metaServiceClient.getParts(tableId);
        List<Object[]> records = new ArrayList<>();
        for(ByteArrayUtils.ComparableByteArray partId : partitions.keySet()) {
            ServiceConnector partConnector = new ServiceConnector(tableId, partitions.get(partId).getReplicates());
            ExecutorApi executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, partConnector);
            List<KeyValue> keyValues = executorApi.getKeyValueByKeyPrefix(null, null, tableId, new byte[]{0});
            for(KeyValue keyValue : keyValues) {
                byte[] oriKey = new byte[keyValue.getKey().length-1];
                System.arraycopy(keyValue.getKey(), 1, oriKey, 0, oriKey.length);
                keyValue.setKey(oriKey);
                records.add(codec.decode(keyValue));
            }
        }
        return records;
    }

    public List<Object[]> getFinishedRecord() throws Exception {
        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableId);
        DingoKeyValueCodec codec = new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions = metaServiceClient.getParts(tableId);
        List<Object[]> records = new ArrayList<>();
        for(ByteArrayUtils.ComparableByteArray partId : partitions.keySet()) {
            ServiceConnector partConnector = new ServiceConnector(tableId, partitions.get(partId).getReplicates());
            ExecutorApi executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, partConnector);
            List<KeyValue> keyValues = executorApi.getKeyValueByKeyPrefix(null, null, tableId, new byte[]{1});
            for(KeyValue keyValue : keyValues) {
                byte[] oriKey = new byte[keyValue.getKey().length-1];
                System.arraycopy(keyValue.getKey(), 1, oriKey, 0, oriKey.length);
                keyValue.setKey(oriKey);
                records.add(codec.decode(keyValue));
            }
        }
        return records;
    }

    public List<KeyValue> getIndexKeyValue(String indexName) throws Exception {
        ServiceConnector serviceConnector = metaServiceClient.getTableConnector(tableId);
        TableApi tableApi = ApiRegistry.getDefault().proxy(TableApi.class, serviceConnector);
        CommonId indexId = tableApi.getIndexId(tableId, indexName);
        if (indexId == null) {
            throw new Exception("Index " + indexName + " not exist");
        }
        ServiceConnector indexServiceConnector = new ServiceConnector(indexId, serviceConnector.getAddresses());
        ExecutorApi executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, indexServiceConnector);
        List<KeyValue> keyValues = executorApi.getAllKeyValue(null, null, indexId);
        return keyValues;
    }

    private Map<String, DingoIndexKeyValueCodec> getDingoIndexCodec(TableDefinition tableDefinition) {

        Map<String, DingoIndexKeyValueCodec> indicsCodec = new HashMap<>();

        tableDefinition.getIndexesMapping().forEach((k, v) -> {
            DingoIndexKeyValueCodec indexCodec = new DingoIndexKeyValueCodec(tableDefinition.getDingoType(),
                tableDefinition.getKeyMapping(), v, tableDefinition.getIndexes().get(k).isUnique());
            indicsCodec.put(k, indexCodec);
        });

        return indicsCodec;
    }
}
