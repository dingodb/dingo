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

package io.dingodb.calcite.schema;

import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.operation.TxnImportDataOperation;
import io.dingodb.calcite.operation.TxnInsertIndexOperation;
import io.dingodb.calcite.runtime.DingoResource;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Parameters;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Op;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class DingoSchema extends AbstractSchema {

    DingoSchema(MetaService metaService, DingoParserContext context, List<String> parent) {
        super(metaService, context, parent);
    }

    public void createTables(@NonNull TableDefinition tableDefinition,
                             @NonNull List<TableDefinition> indexTableDefinitions) {
        metaService.createTables(tableDefinition, indexTableDefinitions);
    }

    public void updateTable(String tableName, Table table) {
        metaService.updateTable(getTableId(tableName), table);
    }

    public boolean dropTable(@NonNull String tableName) {
        return metaService.dropTable(tableName);
    }

    public void createIndex(@NonNull String tableName, @NonNull TableDefinition indexDefinition) {
        CommonId tableId = getTableId(tableName);
        DingoTable table = (DingoTable) getTable(tableName);
        Table definition = table.getTable();
        metaService.createIndex(tableId, TableDefinition.builder().name(definition.name).build(), indexDefinition);
        recreateIndexData(tableName, indexDefinition.getName(), definition);
    }

    public void createDifferenceIndex(String tableName, String indexName, IndexTable indexTable) {
        DingoTable table = (DingoTable) getTable(tableName);
        metaService.createDifferenceIndex(table.getTableId(), table.getIndexId(indexName), indexTable);
        table = (DingoTable) getTable(tableName);
        recreateIndexData(tableName, indexName, table.getTable());
    }

    public void dropIndex(@NonNull String tableName, @NonNull String index) {
        DingoTable table = (DingoTable) getTable(tableName);
        CommonId indexId = table.getIndexTableDefinitions().stream()
            .filter(i -> i.name.equalsIgnoreCase(index.toUpperCase()))
            .findAny().orElseThrow(() -> DingoResource.DINGO_RESOURCE.unknownIndex(index).ex()).tableId;
        metaService.dropIndex(table.getTableId(), indexId);
    }

    private void recreateIndexData(
        @NonNull String tableName, String indexName, @NonNull Table table
    ) {
        try {
            KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(table.version, table.tupleType(), table.keyMapping());
            CommonId tableId = getTableId(tableName);

            Collection<RangeDistribution> ranges = metaService.getRangeDistribution(tableId).values();
            StoreService storeService = StoreService.getDefault();
            ITransaction transaction = getTransaction(tableName);
            for (RangeDistribution range : ranges) {
                StoreInstance store = storeService.getInstance(tableId, range.id());
                long scanTs = Optional.ofNullable(transaction).map(ITransaction::getStartTs).orElse(0L);
                long lockTimeOut = transaction.getLockTimeOut();
                Iterator<KeyValue> iterator = store.txnScan(
                    scanTs,
                    new StoreInstance.Range(range.getStartKey(), range.getEndKey(), true, true),
                    lockTimeOut
                );
                while (iterator.hasNext()) {
                    KeyValue next = iterator.next();
                    insertWithTxn(next, table, indexName, codec);
                }
            }
        } catch (Exception e) {
            log.error("Recreate {} index date failed.", indexName, e);
            dropIndex(tableName, indexName);
            throw new RuntimeException(e);
        }
    }

    public void insertWithTxn(KeyValue keyValue, Table table, String indexName, KeyValueCodec codec) {
        String statementId = UUID.randomUUID().toString();
        Map<String, KeyValue> caches = ExecutionEnvironment.memoryCache
            .computeIfAbsent(statementId, e -> new TreeMap<>());
        Object[] tuples = codec.decode(keyValue);

        CommonId txnId = new CommonId(CommonId.CommonType.TRANSACTION,
            TransactionManager.getServerId().seq, TransactionManager.getStartTs());

        Table newTable = metaService.getTable(table.name);
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distribution = metaService.getRangeDistribution(newTable.tableId);
        recodePriTable(keyValue, txnId, newTable, distribution);
        /*String cacheKey = Base64.getEncoder().encodeToString(keyValue.getKey());
        if (!caches.containsKey(cacheKey)) {
            caches.put(cacheKey, keyValue);
        }*/
        List<IndexTable> indexTableList = newTable.getIndexes();
        if (indexTableList != null) {
            for (IndexTable indexTable : indexTableList) {
                if (!indexTable.getName().equalsIgnoreCase(indexName)) {
                    continue;
                }
                List<Integer> columnIndices = newTable.getColumnIndices(indexTable.columns.stream()
                    .map(Column::getName)
                    .collect(Collectors.toList()));
                Object[] tuplesTmp = columnIndices.stream().map(i -> tuples[i]).toArray();
                KeyValueCodec indexCodec = CodecService.getDefault()
                    .createKeyValueCodec(indexTable.version, indexTable.tupleType(), indexTable.keyMapping());

                keyValue = wrap(indexCodec::encode).apply(tuplesTmp);
                PartitionService ps = PartitionService.getService(
                    Optional.ofNullable(indexTable.getPartitionStrategy())
                        .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
                    metaService.getRangeDistribution(indexTable.tableId);
                CommonId partId = ps.calcPartId(keyValue.getKey(), ranges);
                CodecService.getDefault().setId(keyValue.getKey(), partId.domain);

                byte[] txnIdByte = txnId.encode();
                byte[] tableIdByte = indexTable.tableId.encode();
                byte[] partIdByte = partId.encode();
                keyValue.setKey(
                    ByteUtils.encode(
                        CommonId.CommonType.TXN_CACHE_DATA,
                        keyValue.getKey(),
                        Op.PUTIFABSENT.getCode(),
                        (txnIdByte.length + tableIdByte.length + partIdByte.length),
                        txnIdByte,
                        tableIdByte,
                        partIdByte)
                );
                String cacheKey = Base64.getEncoder().encodeToString(keyValue.getKey());
                if (!caches.containsKey(cacheKey)) {
                    caches.put(cacheKey, keyValue);
                }
            }
        }

        int cacheSize = 0;
        try {
            List<Object[]> tupleList = getCacheTupleList(caches, txnId);
            TxnInsertIndexOperation txnImportDataOperation = new TxnInsertIndexOperation(
                txnId.seq, txnId, 50000
            );
            int result = txnImportDataOperation.insertByTxn(tupleList);
            caches.clear();
        } finally {
            ExecutionEnvironment.memoryCache.remove(statementId);
        }

    }

    public List<Object[]> getCacheTupleList(Map<String, KeyValue> keyValueMap, CommonId txnId) {
        List<Object[]> tupleCacheList = new ArrayList<>();
        for (KeyValue keyValue : keyValueMap.values()) {
            tupleCacheList.add(getCacheTuples(keyValue));
        }
        return tupleCacheList;
    }

    public Object[] getCacheTuples(KeyValue keyValue) {
        return io.dingodb.exec.utils.ByteUtils.decode(keyValue);
    }

    private void recodePriTable(KeyValue keyValue, CommonId txnId, Table table, NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions) {
        CommonId partId = PartitionService.getService(
                Optional.ofNullable(table.getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(keyValue.getKey(), distributions);
        // todo replace
        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
        byte[] txnIdByte = txnId.encode();
        byte[] tableIdByte = table.getTableId().encode();
        byte[] partIdByte = partId.encode();

        keyValue.setKey(ByteUtils.encode(
            CommonId.CommonType.TXN_CACHE_DATA,
            keyValue.getKey(),
            Op.PUTIFABSENT.getCode(),
            (txnIdByte.length + tableIdByte.length + partIdByte.length),
            txnIdByte, tableIdByte, partIdByte));
    }

    private ITransaction getTransaction(String tableName) {
        Table table = Parameters.nonNull(metaService.getTable(tableName), "Table not found.");
        if (table.engine != null && table.engine.contains("TXN")) {
            long startTs = TransactionManager.getStartTs();
            ITransaction transaction = TransactionManager.createTransaction(
                TransactionType.OPTIMISTIC,
                startTs,
                IsolationLevel.SnapshotIsolation.getCode());
            Properties properties = new Properties();
            properties.setProperty("lock_wait_timeout", "50");
            properties.setProperty("transaction_isolation", "REPEATABLE-READ");
            properties.setProperty("transaction_read_only", "off");
            properties.setProperty("txn_mode", "optimistic");
            properties.setProperty("collect_txn", "true");
            properties.setProperty("statement_timeout", "50000");
            properties.setProperty("txn_inert_check", "off");
            properties.setProperty("txn_retry", "off");
            properties.setProperty("txn_retry_cnt", "0");
            transaction.setTransactionConfig(properties);
            return transaction;
        }
        return null;
    }

    public void addDistribution(String tableName, PartitionDetailDefinition partitionDetail) {
        metaService.addDistribution(tableName, partitionDetail);
    }
}
