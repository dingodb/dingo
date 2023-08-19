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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static io.dingodb.client.operation.RangeUtils.mapping;

@Slf4j
public class IndexOperationService {

    private final Map<String, TableInfo> routeTables = new ConcurrentHashMap<>();

    private final MetaServiceConnector metaServiceConnector;
    private final MetaServiceClient rootMetaService;
    private final StoreServiceClient storeService;
    private final int retryTimes;

    public IndexOperationService(String coordinatorSvr, int retryTimes) {
        try {
            DingoConfiguration.parse(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        DingoConfiguration.instance().getStoreOrigin().put("coordinators", coordinatorSvr);
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
        TableInfo tableInfo = Parameters.nonNull(getRouteTable(schemaName, tableName, false), "Table not found.");

        Object[] record = (Object[]) parameters;
        DingoCommonId regionId;
        try {
            regionId = tableInfo.calcRegionId(tableInfo.codec.encode(record).getKey());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        StoreInstance storeInstance = StoreService.getDefault().getInstance(mapping(tableInfo.tableId), mapping(regionId));
        exec(storeInstance, record);

        return storeInstance.insertWithIndex(record);
    }

    private void exec(StoreInstance storeInstance, Object[] record) {
        exec(storeInstance, retryTimes, record).ifPresent(e -> {
            throw new DingoClientException(-1, e);
        });
    }

    private Optional<Throwable> exec(
        StoreInstance storeInstance,
        int retry,
        Object[] record
    ) {
        if (retry <= 0) {
            return Optional.of(new DingoClientException(-1, "Exceeded the retry limit for performing " + PutOperation.getInstance().getClass()));
        }
        Optional<Throwable> error = Optional.empty();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        CompletableFuture.runAsync(() -> storeInstance.insertIndex(record), Executors.executor("exec-operator"))
            .thenApply(r -> Optional.<Throwable>empty())
            .exceptionally(Optional::of)
            .thenAccept(e -> {
                e.map(OperationUtils::getCause)
                    .map(err -> {
                        storeInstance.insertIndex(record);
                        return exec(storeInstance, retry - 1, record).orNull();
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

    private MetaServiceClient getSubMetaService(String schemaName) {
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
            NavigableMap<ComparableByteArray, RangeDistribution> parts = metaService.getRangeDistribution(tableId);
            KeyValueCodec keyValueCodec = new KeyValueCodec(DingoKeyValueCodec.of(tableId.entityId(), table), table);

            return new TableInfo(schemaName, tableName, tableId, table, keyValueCodec, parts);
        } catch (Exception e) {
            log.error("Refresh route table failed, schema: {}, table: {}", schemaName, tableName, e);
            return null;
        }
    }

}
