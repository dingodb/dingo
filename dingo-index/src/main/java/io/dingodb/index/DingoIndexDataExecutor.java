package io.dingodb.index;

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

    private CoordinatorConnector coordinatorConnector;

    public DingoIndexDataExecutor(String coordinatorSrvAddresses) {
        this.coordinatorConnector = CoordinatorConnector.getCoordinatorConnector(coordinatorSrvAddresses);
    }

    public DingoIndexDataExecutor(CoordinatorConnector coordinatorConnector) {
        this.coordinatorConnector = coordinatorConnector;
    }

    public void executeInsert(String tableName, Object[] record) throws Exception {
        MetaServiceClient metaServiceClient = new MetaServiceClient(coordinatorConnector);
        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableName);

        int tableDefinitionVersion = tableDefinition.getVersion();

        CommonId tableId = metaServiceClient.getTableId(tableName);

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
            metaServiceClient.getParts(tableName);

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
            KeyValue IndexKeyValue = indexCodec.encode(record);
            CommonId indexId = tableApi.getIndexId(tableId, indexName);
            ServiceConnector indexServiceConnector = new ServiceConnector(indexId, serviceConnector.getAddresses());
            ExecutorApi executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, indexServiceConnector);
            boolean b = executorApi.upsertKeyValue(null, null, indexId, IndexKeyValue);
        }

        TableDefinition currentTd = metaServiceClient.getTableDefinition(tableName);

        if (currentTd.getVersion() != tableDefinitionVersion) {
            throw new Exception("table definition changed");
        }

        boolean b2 = finishedExecutorcApi.upsertKeyValue(null, null, tableId, finishedKeyValue);
        boolean b3 = unfinishedExecutorcApi.delete(null, null, tableId, unfinishedKeyValue.getPrimaryKey());
    }

    public void executeInsertIndex(String tableName, Object[] record, String indexName) throws Exception {
        MetaServiceClient metaServiceClient = new MetaServiceClient(coordinatorConnector);
        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableName);

        int tableDefinitionVersion = tableDefinition.getVersion();

        CommonId tableId = metaServiceClient.getTableId(tableName);

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
            metaServiceClient.getParts(tableName);

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

        TableDefinition currentTd = metaServiceClient.getTableDefinition(tableName);

        if (currentTd.getVersion() != tableDefinitionVersion) {
            throw new Exception("table definition changed");
        }

        boolean b2 = finishedExecutorcApi.upsertKeyValue(null, null, tableId, finishedKeyValue);
        boolean b3 = unfinishedExecutorcApi.delete(null, null, tableId, unfinishedKeyValue.getPrimaryKey());
    }

    public List<byte[]> getKeyByteByIndex(String tableName, String indexName, Object[] record) throws Exception {
        MetaServiceClient metaServiceClient = new MetaServiceClient(coordinatorConnector);
        CommonId tableId = metaServiceClient.getTableId(tableName);
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

    public List<Object[]> getRecordsByIndex(String tableName, String indexName, Object[] record) throws Exception {
        MetaServiceClient metaServiceClient = new MetaServiceClient(coordinatorConnector);
        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableName);
        CommonId tableId = metaServiceClient.getTableId(tableName);
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
                metaServiceClient.getParts(tableName);
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

    public List<Object[]> getUnfinishedRecord(String tableName) throws Exception {
        MetaServiceClient metaServiceClient = new MetaServiceClient(coordinatorConnector);
        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableName);
        CommonId tableId = metaServiceClient.getTableId(tableName);
        DingoKeyValueCodec codec = new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions = metaServiceClient.getParts(tableName);
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

    public List<Object[]> getFinishedRecord(String tableName) throws Exception {
        MetaServiceClient metaServiceClient = new MetaServiceClient(coordinatorConnector);
        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableName);
        CommonId tableId = metaServiceClient.getTableId(tableName);
        DingoKeyValueCodec codec = new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions = metaServiceClient.getParts(tableName);
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

    public List<KeyValue> getIndexKeyValue(String tableName, String indexName) throws Exception {
        MetaServiceClient metaServiceClient = new MetaServiceClient(coordinatorConnector);
        CommonId tableId = metaServiceClient.getTableId(tableName);
        ServiceConnector serviceConnector = metaServiceClient.getTableConnector(tableId);
        TableApi tableApi = ApiRegistry.getDefault().proxy(TableApi.class, serviceConnector);
        CommonId indexId = tableApi.getIndexId(tableId, indexName);
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
