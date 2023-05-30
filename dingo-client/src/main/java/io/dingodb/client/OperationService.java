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
import io.dingodb.client.common.RouteTable;
import io.dingodb.client.operation.impl.Operation;
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
import io.dingodb.sdk.common.utils.ByteArrayUtils;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

@Slf4j
public class OperationService {

    private static Map<DingoCommonId, RouteTable> dingoRouteTables = new ConcurrentHashMap<>();

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
        MetaServiceClient metaService = Parameters
            .nonNull(rootMetaService.getSubMetaService(schemaName), "Schema not found: " + schemaName);
        DingoCommonId tableId = Parameters.nonNull(metaService.getTableId(tableName), "Table not found: " + tableName);
        RouteTable routeTable = getAndRefreshRouteTable(metaService, tableId, false);
        Table table = routeTable.table;

        Operation.Fork fork = operation.fork(Any.wrap(parameters), table, routeTable);

        exec(operation, metaService, tableId, table, routeTable, fork, retryTimes).ifPresent(e -> {
            if (!fork.isIgnoreError()) {
                throw new DingoClientException(-1, e);
            }
        });

        return operation.reduce(fork);
    }

    private Optional<Throwable> exec(
        Operation operation,
        MetaServiceClient metaService,
        DingoCommonId tableId,
        Table table,
        RouteTable routeTable,
        Operation.Fork fork,
        int retry
    ) {
        if (retry <= 0) {
            return Optional.of(new RuntimeException("Exceeded the retry limit for performing " + operation.getClass()));
        }
        List<OperationContext> contexts = generateContext(tableId, table, routeTable.codec, fork);
        Optional<Throwable> error = Optional.empty();
        CountDownLatch countDownLatch = new CountDownLatch(contexts.size());
        contexts.forEach(context -> CompletableFuture
            .runAsync(() -> operation.exec(context), Executors.executor("exec-operator"))
            .thenApply(r -> Optional.<Throwable>empty())
            .exceptionally(Optional::of)
            .thenAccept(e -> {
                e.filter(DingoClientException.InvalidRouteTableException.class::isInstance).map(err -> {
                    RouteTable newRouteTable = getAndRefreshRouteTable(metaService, tableId, true);
                    Operation.Fork newFork = operation.fork(context, newRouteTable);
                    return exec(operation, metaService, tableId, table, newRouteTable, newFork, retry - 1).orNull();
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

    private List<OperationContext> generateContext(
        DingoCommonId tableId, Table table, KeyValueCodec codec, Operation.Fork fork
    ) {
        int i = 0;
        List<OperationContext> contexts = new ArrayList<>(fork.getSubTasks().size());
        for (Operation.Task subTask : fork.getSubTasks()) {
            contexts.add(new OperationContext(
                tableId, subTask.getRegionId(), table, codec, storeService, i++, subTask.getParameters(), Any.wrap(fork.result())
            ));
        }
        return contexts;
    }

    public synchronized boolean createTable(String schema, String name, Table table) {
        MetaServiceClient metaService = Parameters
            .nonNull(rootMetaService.getSubMetaService(schema), "Schema not found: " + schema);
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
            .map(col -> DingoTypeFactory.fromName(col.getType(), col.getElementType(), col.isNullable()))
            .forEach(keyTypes::add);
        DefinitionUtils.checkAndConvertRangePartition(
            keyNames,
            table.getPartition().cols(),
            keyTypes,
            table.getPartition().details().stream().map(PartitionDetail::getOperand).collect(Collectors.toList())
        );
    }

    public boolean dropTable(String schema, String tableName) {
        MetaServiceClient metaService = Parameters
            .nonNull(rootMetaService.getSubMetaService(schema), "Schema not found: " + schema);
        return metaService.dropTable(tableName);
    }

    public Table getTableDefinition(String schema, String tableName) {
        MetaServiceClient metaService = Parameters
            .nonNull(rootMetaService.getSubMetaService(schema), "Schema not found: " + schema);
        return metaService.getTableDefinition(tableName);
    }

    public synchronized RouteTable getAndRefreshRouteTable(
        MetaServiceClient metaService, DingoCommonId tableId, boolean isRefresh
    ) {
        if (isRefresh) {
            dingoRouteTables.remove(tableId);
        }
        RouteTable routeTable = dingoRouteTables.get(tableId);
        if (routeTable == null) {
            Table table = metaService.getTableDefinition(tableId);
            if (table == null) {
                return null;
            }

            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> parts =
                    metaService.getRangeDistribution(table.getName());

            KeyValueCodec keyValueCodec = new io.dingodb.client.common.KeyValueCodec(
                DingoKeyValueCodec.of(tableId.entityId(), table),
                table
            );

            routeTable = new RouteTable(tableId, table, keyValueCodec, parts);

            dingoRouteTables.put(tableId, routeTable);
        }

        return routeTable;
    }

}
