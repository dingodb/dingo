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

package io.dingodb.client;

import io.dingodb.client.common.KeyValueCodec;
import io.dingodb.client.common.TableInfo;
import io.dingodb.client.operation.impl.Operation;
import io.dingodb.client.utils.OperationUtils;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.util.DefinitionUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.partition.PartitionDetail;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.utils.ByteArrayUtils.ComparableByteArray;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.sdk.service.connector.MetaServiceConnector;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import io.dingodb.sdk.service.store.StoreServiceClient;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

@Slf4j
public class OperationService {

    private final Map<String, TableInfo> routeTables = new ConcurrentHashMap<>();

    private final MetaServiceConnector metaServiceConnector;
    private final MetaServiceClient rootMetaService;
    private final StoreServiceClient storeService;
    private final int retryTimes;

    public OperationService(String coordinatorSvr, int retryTimes) {
        this.rootMetaService = new MetaServiceClient(coordinatorSvr);
        this.metaServiceConnector = (MetaServiceConnector) rootMetaService.getMetaConnector();
        this.storeService = new StoreServiceClient(rootMetaService, retryTimes);
        this.retryTimes = retryTimes;
    }

    public void init() {
    }

    public void close() {
        storeService.shutdown();
    }

    public <R> R exec(String schemaName, String tableName, Operation operation, Object parameters) {
        schemaName = schemaName.toUpperCase();
        TableInfo tableInfo = Parameters.nonNull(getRouteTable(schemaName, tableName, false), "Table not found.");

        Operation.Fork fork;
        try {
            fork = operation.fork(Any.wrap(parameters), tableInfo);
        } catch (Exception ignore) {
            tableInfo = Parameters.nonNull(getRouteTable(schemaName, tableName, true), "Table not found.");
            fork = operation.fork(Any.wrap(parameters), tableInfo);
        }

        exec(tableInfo, operation, fork);

        return operation.reduce(fork);
    }

    private void exec(TableInfo tableInfo, Operation operation, Operation.Fork fork) {
        exec(operation, tableInfo, fork, retryTimes).ifPresent(e -> {
            if (!fork.isIgnoreError()) {
                throw new DingoClientException(-1, e);
            }
        });
    }

    private Optional<Throwable> exec(
        Operation operation,
        TableInfo tableInfo,
        Operation.Fork fork,
        int retry
    ) {
        if (retry <= 0) {
            return Optional.of(new DingoClientException(-1, "Exceeded the retry limit for performing "
                + operation.getClass()));
        }
        List<OperationContext> contexts = generateContext(tableInfo, fork);
        Optional<Throwable> error = Optional.empty();
        CountDownLatch countDownLatch = new CountDownLatch(contexts.size());
        contexts.forEach(context -> CompletableFuture
            .runAsync(() -> operation.exec(context), Executors.executor("exec-operator"))
            .thenApply(r -> Optional.<Throwable>empty())
            .exceptionally(Optional::of)
            .thenAccept(e -> {
                e.map(OperationUtils::getCause)
                    .ifPresent(__ -> log.error(__.getMessage(), __))
                    .filter(DingoClientException.InvalidRouteTableException.class::isInstance)
                    .map(err -> {
                        TableInfo newTableInfo = getRouteTable(
                            tableInfo.schemaName.toUpperCase(),
                            tableInfo.tableName,
                            true);
                        Operation.Fork newFork = operation.fork(context, newTableInfo);
                        if (newFork == null) {
                            return exec(operation, newTableInfo, newFork, 0).orNull();
                        }
                        return exec(operation, newTableInfo, newFork, retry - 1).orNull();
                    }).ifPresent(error::ifAbsentSet);
                countDownLatch.countDown();
            }));
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.warn("Exec {} interrupted.", operation.getClass());
        }
        return error;
    }

    private List<OperationContext> generateContext(TableInfo table, Operation.Fork fork) {
        int i = 0;
        List<OperationContext> contexts = new ArrayList<>(fork.getSubTasks().size());
        for (Operation.Task subTask : fork.getSubTasks()) {
            contexts.add(OperationContext.builder()
                .tableId(table.tableId)
                .regionId(subTask.getRegionId())
                .table(table.definition)
                .codec(table.codec)
                .storeService(storeService)
                .seq(i++)
                .parameters(subTask.getParameters())
                .result(fork.getResultRef())
                .build()
            );
        }
        return contexts;
    }

    public synchronized boolean createTable(String schema, String name, Table table) {
        MetaServiceClient metaService = getSubMetaService(schema);
        Optional.ifPresent(table.getPartition(), __ -> checkAndConvertRangePartition(table));
        return metaService.createTable(name, table);
    }

    private void checkAndConvertRangePartition(Table table) {
        List<Column> columns = table.getColumns();
        List<String> keyNames = new ArrayList<>();
        List<DingoType> keyTypes = new ArrayList<>();
        columns.stream().filter(Column::isPrimary)
            .sorted(Comparator.comparingInt(Column::getPrimary))
            .peek(col -> keyNames.add(col.getName()))
            .map(col -> DingoTypeFactory.INSTANCE.fromName(col.getType(), col.getElementType(), col.isNullable()))
            .forEach(keyTypes::add);
        DefinitionUtils.checkAndConvertRangePartition(
            keyNames,
            table.getPartition().getCols(),
            keyTypes,
            table.getPartition().getDetails().stream().map(PartitionDetail::getOperand).collect(Collectors.toList())
        );
    }

    public boolean dropTable(String schema, String tableName) {
        MetaServiceClient metaService = getSubMetaService(schema);
        routeTables.remove(schema.toUpperCase() + "." + tableName);
        return metaService.dropTable(tableName);
    }

    public boolean dropTables(String schema, List<String> tableNames) {
        MetaServiceClient metaService = getSubMetaService(schema);
        tableNames.forEach(t -> routeTables.remove(schema.toUpperCase() + "." + t));
        return metaService.dropTables(tableNames);
    }

    public Table getTableDefinition(String schemaName, String tableName) {
        return Parameters.nonNull(
            getRouteTable(schemaName.toUpperCase(), tableName, true), "Table not found.").definition;
    }

    /**
     * getTable.
     * @param schemaName schema name
     * @param tableName table name
     * @return table and indexes
     */
    public List<Table> getTables(String schemaName, String tableName) {
        MetaServiceClient metaService = getSubMetaService(schemaName);
        return metaService.getTables(tableName);
    }

    /** getTableIndex.
     * @param schemaName schema name
     * @param tableName table name
     * @return indexes
     */
    public List<Table> getTableIndexes(String schemaName, String tableName) {
        MetaServiceClient metaService = getSubMetaService(schemaName);
        return (List<Table>) metaService.getTableIndexes(tableName).values();
    }

    private MetaServiceClient getSubMetaService(String schemaName) {
        schemaName = schemaName.toUpperCase();
        return Parameters.nonNull(rootMetaService.getSubMetaService(schemaName), "Schema not found: " + schemaName);
    }

    private TableInfo getRouteTable(String schemaName, String tableName, boolean forceRefresh) {
        return routeTables.compute(
            schemaName + "." + tableName,
            (k, v) -> Parameters.cleanNull(forceRefresh ? null : v, () -> refreshRouteTable(schemaName, tableName))
        );
    }

    private TableInfo refreshRouteTable(String schemaName, String tableName) {
        try {
            MetaServiceClient metaService = getSubMetaService(schemaName);

            DingoCommonId tableId = Parameters.nonNull(metaService.getTableId(tableName), "Table not found.");
            Table table = Parameters.nonNull(metaService.getTableDefinition(tableName), "Table not found.");
            NavigableMap<ComparableByteArray, RangeDistribution> parts = new TreeMap<>();
            metaService.getRangeDistribution(tableId)
                .forEach((k, v) -> parts.put(new ComparableByteArray(k.getBytes(), k.isIgnoreLen(), k.getPos()), v));
            KeyValueCodec keyValueCodec = new KeyValueCodec(DingoKeyValueCodec.of(tableId.entityId(), table), table);

            return new TableInfo(schemaName, tableName, tableId, table, keyValueCodec, parts);
        } catch (Exception e) {
            log.error("Refresh route table failed, schema: {}, table: {}", schemaName, tableName, e);
            return null;
        }
    }

}
