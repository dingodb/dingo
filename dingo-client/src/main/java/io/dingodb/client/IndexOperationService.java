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
import io.dingodb.client.operation.impl.CompareAndSetOperation;
import io.dingodb.client.operation.impl.DeleteOperation;
import io.dingodb.client.operation.impl.Operation;
import io.dingodb.client.operation.impl.PutOperation;
import io.dingodb.client.utils.OperationUtils;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.util.Optional;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.ByteArrayUtils.ComparableByteArray;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.sdk.service.connector.MetaServiceConnector;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import io.dingodb.sdk.service.store.StoreServiceClient;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.dingodb.client.operation.RangeUtils.mapping;

@Slf4j
public class IndexOperationService {

    private final Map<String, TableInfo> routeTables = new ConcurrentHashMap<>();

    private final MetaServiceConnector metaServiceConnector;
    private final MetaServiceClient rootMetaService;
    private final StoreServiceClient storeService;
    private final int retryTimes;

    public static class Parameter {
        public final Object[] record;
        public final Object[] expect;

        public Parameter(Object[] record) {
            this(record, null);
        }

        public Parameter(Object[] record, Object[] expect) {
            this.record = record;
            this.expect = expect;
        }
    }

    public IndexOperationService(String coordinatorSvr, int retryTimes) {
        try {
            DingoConfiguration.parse(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        DingoConfiguration.instance().getConfigMap("store").put("coordinators", coordinatorSvr);
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

    public boolean exec(String schemaName, String tableName, Operation operation, Object parameters) {
        TableInfo tableInfo = Parameters.nonNull(
            getRouteTable(schemaName.toUpperCase(), tableName, false), "Table not found.");

        Parameter parameter = (Parameter) parameters;
        DingoCommonId regionId;

        regionId = tableInfo.calcRegionId(tableInfo.codec.encodeKey(parameter.record));

        Function<Operation, Class<? extends Operation>> func = Operation::getClass;
        StoreInstance storeInstance =
            StoreService.getDefault().getInstance(mapping(tableInfo.tableId), mapping(regionId));
        exec(storeInstance, parameter, operation, func);

        Predicate<Class<? extends Operation>> exec = __ -> secondExec(storeInstance, parameter, __);
        return exec.test(func.apply(operation));
    }

    private void exec(StoreInstance storeInstance,
                      Parameter parameter,
                      Operation operation,
                      Function<Operation, Class<? extends Operation>> func) {
        exec(storeInstance, parameter, operation, func, retryTimes).ifPresent(e -> {
            throw new DingoClientException(-1, e);
        });
    }

    private Optional<Throwable> exec(
        StoreInstance storeInstance,
        Parameter parameter,
        Operation operation,
        Function<Operation, Class<? extends Operation>> func,
        int retry
    ) {
        if (retry <= 0) {
            return Optional.of(new DingoClientException(
                -1, "Exceeded the retry limit for performing " + PutOperation.getInstance().getClass()));
        }
        Optional<Throwable> error = Optional.empty();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Predicate<Class<? extends Operation>> exec = __ -> firstExec(storeInstance, parameter, __);
        CompletableFuture.runAsync(() -> exec.test(func.apply(operation)), Executors.executor("exec-operator"))
            .thenApply(r -> Optional.<Throwable>empty())
            .exceptionally(Optional::of)
            .thenAccept(e -> {
                e.map(OperationUtils::getCause)
                    .ifPresent(__ -> log.error(__.getMessage(), __))
                    .map(err -> {
                        exec.test(func.apply(operation));
                        return exec(storeInstance, parameter, operation, func, retry - 1).orNull();
                    }).ifPresent(error::ifAbsentSet);
                countDownLatch.countDown();
            });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.warn("Exec {} interrupted.", PutOperation.getInstance().getClass());
        }
        return error;
    }

    private static boolean secondExec(
        StoreInstance storeInstance,
        Parameter parameter,
        Class<? extends Operation> opClass
    ) {
        if (opClass == PutOperation.class) {
            return storeInstance.insertWithIndex(parameter.record);
        }
        if (opClass == DeleteOperation.class) {
            return storeInstance.deleteIndex(parameter.record);
        }
        if (opClass == CompareAndSetOperation.class) {
            boolean result = storeInstance.updateWithIndex(parameter.expect, parameter.record);
            storeInstance.deleteIndex(parameter.expect, parameter.record);
            return result;
        }
        return false;
    }

    private static boolean firstExec(
        StoreInstance storeInstance,
        Parameter parameter,
        Class<? extends Operation> opClass
    ) {
        if (opClass == PutOperation.class) {
            return storeInstance.insertIndex(parameter.record);
        }
        if (opClass == DeleteOperation.class) {
            return storeInstance.deleteWithIndex(parameter.record);
        }
        if (opClass == CompareAndSetOperation.class) {
            return storeInstance.insertIndex(parameter.expect);
        }
        return false;
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
