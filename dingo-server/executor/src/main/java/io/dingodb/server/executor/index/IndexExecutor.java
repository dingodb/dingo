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
import io.dingodb.common.codec.CodeTag;
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
import io.dingodb.server.client.connector.impl.ServiceConnector;
import io.dingodb.server.client.meta.service.MetaServiceClient;
import io.dingodb.server.client.meta.service.MetaServiceClientProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class IndexExecutor {

    private MetaServiceClient metaService;

    private CommonId tableId;

    private final byte deleteKey = CodeTag.DELETEFLAG;

    private final byte unfinishKey = CodeTag.UNFINISHFALG;

    private final byte finishedKey = CodeTag.FINISHEDFALG;

    private Map<Integer, KeyValueCodec> codecMap = new HashMap<>();

    private Map<Integer, RangeStrategy> rangeStrategyMap = new HashMap<>();

    NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions = null;

    private Map<ByteArrayUtils.ComparableByteArray, ExecutorApi> executorApiMap = new HashMap<>();

    private Map<Integer, Map<String, DingoIndexKeyValueCodec>> indexCodecMap = new HashMap<>();

    private Map<String, CommonId> indexNameIdMap = new HashMap<>();

    private Map<String, ExecutorApi> indexExecutorApiMap = new HashMap<>();

    private ServiceConnector serviceConnector = null;

    private TableApi tableApi = null;

    public IndexExecutor(CommonId tableId) {
        this.tableId = tableId;
        this.metaService = MetaServiceClientProvider.META_SERVICE_CLIENT;
    }

    public IndexExecutor(CommonId tableId, MetaServiceClient metaService) {
        this.tableId = tableId;
        this.metaService = metaService;
    }

    public boolean insertIndex(Object[] row, TableDefinition tableDefinition, String indexName) {
        return opIndexData(row, tableDefinition, indexName, 1);
    }

    public boolean deleteFromIndex(Object[] row, TableDefinition tableDefinition, String indexName) {
        return opIndexData(row, tableDefinition, indexName, -1);
    }

    private boolean opIndexData(Object[] row, TableDefinition tableDefinition, String indexName, int op) {
        int version = tableDefinition.getVersion();
        Map<String, DingoIndexKeyValueCodec> indicsCodec;
        if (indexCodecMap.containsKey(version)) {
            indicsCodec = indexCodecMap.get(version);
        } else {
            indicsCodec = new HashMap<>();
            tableDefinition.getIndexesMapping().forEach((k, v) -> {
                DingoIndexKeyValueCodec indexCodec = new DingoIndexKeyValueCodec(tableDefinition.getDingoType(),
                    tableDefinition.getKeyMapping(), v, tableDefinition.getIndexes().get(k).isUnique());
                indicsCodec.put(k, indexCodec);
            });
            indexCodecMap.put(version, indicsCodec);
        }

        DingoIndexKeyValueCodec indexCodec = indicsCodec.get(indexName);
        KeyValue indexKeyValue = null;
        try {
            indexKeyValue = indexCodec.encode(row);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (tableApi == null) {
            if (serviceConnector == null) {
                serviceConnector = metaService.getTableConnector(tableId);
            }
            tableApi = ApiRegistry.getDefault().proxy(TableApi.class, serviceConnector);
        }
        CommonId indexId;
        if (indexNameIdMap.containsKey(indexName)) {
            indexId = indexNameIdMap.get(indexName);
        } else {
            indexId = tableApi.getIndexId(tableId, indexName);
            if (indexId == null) {
                throw new RuntimeException("index " + indexName + "not exist");
            }
            indexNameIdMap.put(indexName, indexId);
        }
        ExecutorApi executorApi;
        if (indexExecutorApiMap.containsKey(indexName)) {
            executorApi = indexExecutorApiMap.get(indexName);
        } else {
            if (serviceConnector == null) {
                serviceConnector = metaService.getTableConnector(tableId);
                if (serviceConnector == null) {
                    throw new RuntimeException("table " + tableId + "not exist");
                }
            }
            ServiceConnector indexServiceConnector = new ServiceConnector(indexId, serviceConnector.getAddresses());
            executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, indexServiceConnector);
            indexExecutorApiMap.put(indexName, executorApi);
        }
        if (op == -1) {
            if (!executorApi.delete(null, null, indexId, indexKeyValue.getPrimaryKey())) {
                return false;
            }
        } else if (op == 1) {
            if (!executorApi.upsertKeyValue(null, null, indexId, indexKeyValue)) {
                return false;
            }
        }
        return true;
    }

    public KeyValue getOriKV(Object[] row, TableDefinition tableDefinition) {
        int version = tableDefinition.getVersion();
        KeyValueCodec codec;
        if (codecMap.containsKey(version)) {
            codec = codecMap.get(version);
        } else {
            codec = new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
            codecMap.put(version, codec);
        }
        try {
            return codec.encode(row);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Object[] getRow(KeyValue kv, TableDefinition tableDefinition) {
        int version = tableDefinition.getVersion();
        KeyValueCodec codec;
        if (codecMap.containsKey(version)) {
            codec = codecMap.get(version);
        } else {
            codec = new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
            codecMap.put(version, codec);
        }
        try {
            return codec.decode(kv);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public KeyValue getUnfinishKV(KeyValue oriKV) {
        return getNewKV(oriKV, unfinishKey);
    }

    public KeyValue getFinishedKV(KeyValue oriKV) {
        return getNewKV(oriKV, finishedKey);
    }

    public KeyValue getDeleteKV(KeyValue oriKV) {
        return getNewKV(oriKV, deleteKey);
    }

    public KeyValue getNewKV(KeyValue oriKV, byte flag) {
        oriKV.getKey()[0] = flag;
        return oriKV;
    }

    public ExecutorApi getExecutor(byte[] key, TableDefinition tableDefinition) {
        int version = tableDefinition.getVersion();
        RangeStrategy rangeStrategy;
        if (partitions == null) {
            partitions = metaService.getParts(tableId);
        }
        if (rangeStrategyMap.containsKey(version)) {
            rangeStrategy = rangeStrategyMap.get(version);
        } else {
            rangeStrategy = new RangeStrategy(tableDefinition, partitions.navigableKeySet());
            rangeStrategyMap.put(version, rangeStrategy);
        }

        ByteArrayUtils.ComparableByteArray partId = rangeStrategy.calcPartId(key);
        ExecutorApi executorcApi;
        if (executorApiMap.containsKey(partId)) {
            executorcApi = executorApiMap.get(partId);
        } else {
            ServiceConnector unfinishedPartConnector = new ServiceConnector(
                partitions.get(partId).getId(), partitions.get(partId).getReplicates()
            );
            executorcApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, unfinishedPartConnector);
            executorApiMap.put(partId, executorcApi);
        }
        return executorcApi;
    }

    public List<Object[]> getRowByIndex(Object[] row, TableDefinition tableDefinition, String indexName) {
        int version = tableDefinition.getVersion();
        KeyValueCodec codec;
        if (codecMap.containsKey(version)) {
            codec = codecMap.get(version);
        } else {
            codec = new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
            codecMap.put(version, codec);
        }

        Map<String, DingoIndexKeyValueCodec> indicsCodec;
        if (indexCodecMap.containsKey(version)) {
            indicsCodec = indexCodecMap.get(version);
        } else {
            indicsCodec = new HashMap<>();
            tableDefinition.getIndexesMapping().forEach((k, v) -> {
                DingoIndexKeyValueCodec indexCodec = new DingoIndexKeyValueCodec(tableDefinition.getDingoType(),
                    tableDefinition.getKeyMapping(), v, tableDefinition.getIndexes().get(k).isUnique());
                indicsCodec.put(k, indexCodec);
            });
            indexCodecMap.put(version, indicsCodec);
        }

        DingoIndexKeyValueCodec indexCodec = indicsCodec.get(indexName);
        byte[] indexKey = null;
        try {
            indexKey = indexCodec.encodeIndexKey(row);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (tableApi == null) {
            if (serviceConnector == null) {
                serviceConnector = metaService.getTableConnector(tableId);
            }
            tableApi = ApiRegistry.getDefault().proxy(TableApi.class, serviceConnector);
        }
        CommonId indexId;
        if (indexNameIdMap.containsKey(indexName)) {
            indexId = indexNameIdMap.get(indexName);
        } else {
            indexId = tableApi.getIndexId(tableId, indexName);
            if (indexId == null) {
                throw new RuntimeException("index " + indexName + " not exist");
            }
            indexNameIdMap.put(indexName, indexId);
        }
        ExecutorApi executorApi;
        if (indexExecutorApiMap.containsKey(indexName)) {
            executorApi = indexExecutorApiMap.get(indexName);
        } else {
            if (serviceConnector == null) {
                serviceConnector = metaService.getTableConnector(tableId);
                if (serviceConnector == null) {
                    throw new RuntimeException("table " + tableId + " not exist");
                }
            }
            ServiceConnector indexServiceConnector = new ServiceConnector(indexId, serviceConnector.getAddresses());
            executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, indexServiceConnector);
            indexExecutorApiMap.put(indexName, executorApi);
        }

        List<KeyValue> keyValues = executorApi.getKeyValueByKeyPrefix(null, null, indexId, indexKey);
        List<Object[]> records = new ArrayList<>();
        for (KeyValue keyValue : keyValues) {
            byte[] key;
            try {
                key = indexCodec.decodeKeyBytes(keyValue);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            ExecutorApi tableExecutorApi = getExecutor(key, tableDefinition);
            byte[] value = tableExecutorApi.getValueByPrimaryKey(null, null, tableId, key);
            KeyValue oriKeyValue = new KeyValue(key, value);
            try {
                records.add(codec.decode(oriKeyValue));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return records;
    }

    public List<Object[]> getFinishedRecords() {
        return getRecordsByPrefix(new byte[] {finishedKey});
    }

    public List<Object[]> getUnfinishRecords() {
        return getRecordsByPrefix(new byte[] {unfinishKey});
    }

    public List<Object[]> getDeleteRecords() {
        return getRecordsByPrefix(new byte[] {deleteKey});
    }

    private List<Object[]> getRecordsByPrefix(byte[] prefix) {
        TableDefinition tableDefinition = metaService.getTableDefinition(tableId);
        int version = tableDefinition.getVersion();
        KeyValueCodec codec;
        if (codecMap.containsKey(version)) {
            codec = codecMap.get(version);
        } else {
            codec = new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
            codecMap.put(version, codec);
        }
        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> partitions = metaService.getParts(tableId);
        List<Object[]> records = new ArrayList<>();
        for (ByteArrayUtils.ComparableByteArray partId : partitions.keySet()) {
            Part part = partitions.get(partId);
            ServiceConnector partConnector = new ServiceConnector(
                part.getId(), part.getReplicates()
            );
            ExecutorApi executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, partConnector);
            List<KeyValue> keyValues = executorApi.getKeyValueByRange(
                null, null, tableId, prefix, null
            );
            for (KeyValue keyValue : keyValues) {
                try {
                    records.add(codec.decode(keyValue));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return records;
    }

    public List<KeyValue> getIndexKeyValue(String indexName) {
        if (tableApi == null) {
            if (serviceConnector == null) {
                serviceConnector = metaService.getTableConnector(tableId);
                if (serviceConnector == null) {
                    throw new RuntimeException("table " + tableId + " not exist");
                }
            }
            tableApi = ApiRegistry.getDefault().proxy(TableApi.class, serviceConnector);
        }
        CommonId indexId;
        if (indexNameIdMap.containsKey(indexName)) {
            indexId = indexNameIdMap.get(indexName);
        } else {
            indexId = tableApi.getIndexId(tableId, indexName);
            if (indexId == null) {
                return Collections.emptyList();
            }
            indexNameIdMap.put(indexName, indexId);
        }
        ExecutorApi executorApi;
        if (indexExecutorApiMap.containsKey(indexName)) {
            executorApi = indexExecutorApiMap.get(indexName);
        } else {
            if (serviceConnector == null) {
                serviceConnector = metaService.getTableConnector(tableId);
                if (serviceConnector == null) {
                    throw new RuntimeException("table " + tableId + " not exist");
                }
            }
            ServiceConnector indexServiceConnector = new ServiceConnector(indexId, serviceConnector.getAddresses());
            executorApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, indexServiceConnector);
            indexExecutorApiMap.put(indexName, executorApi);
        }

        return executorApi.getAllKeyValue(null, null, indexId);
    }

    public void refresh() {
        partitions = null;
        executorApiMap.clear();
        indexNameIdMap.clear();
        indexExecutorApiMap.clear();
        serviceConnector = null;
        tableApi = null;
    }
}
