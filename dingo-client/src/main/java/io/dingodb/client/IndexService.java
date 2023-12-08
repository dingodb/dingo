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

import io.dingodb.client.common.IndexInfo;
import io.dingodb.client.common.KeyValueCodec;
import io.dingodb.client.operation.impl.Operation;
import io.dingodb.client.utils.OperationUtils;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.util.Optional;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.index.Index;
import io.dingodb.sdk.common.index.IndexMetrics;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.serial.schema.LongSchema;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.sdk.service.index.IndexServiceClient;
import io.dingodb.sdk.service.meta.AutoIncrementService;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static io.dingodb.sdk.common.utils.EntityConversion.mapping;
import static io.dingodb.sdk.common.utils.Parameters.cleanNull;

@Slf4j
public class IndexService {

    private final Map<String, IndexInfo> routeTables = new ConcurrentHashMap<>();

    private final MetaServiceClient rootMetaService;
    private final IndexServiceClient indexService;
    private final AutoIncrementService autoIncrementService;
    private final int retryTimes;
    private final DingoSchema<Long> schema = new LongSchema(0);
    private final DingoType dingoType = new LongType(false);

    public IndexService(String coordinatorSvr, AutoIncrementService autoIncrementService, int retryTimes) {
        this.rootMetaService = new MetaServiceClient(coordinatorSvr);
        this.indexService = new IndexServiceClient(rootMetaService, retryTimes);
        this.autoIncrementService = autoIncrementService;
        this.retryTimes = retryTimes;
    }

    public <R> R exec(String schemaName, String indexName, Operation operation, Object parameters) {
        return exec(schemaName, indexName, operation, parameters, VectorContext.builder().build());
    }

    public <R> R exec(
        String schemaName,
        String indexName,
        Operation operation,
        Object parameters,
        VectorContext context
    ) {
        schemaName = schemaName.toUpperCase();
        IndexInfo indexInfo = Parameters.nonNull(getRouteTable(schemaName, indexName, false), "Index not found.");

        Operation.Fork fork = null;
        try {
            fork = operation.fork(Any.wrap(parameters), indexInfo);
        } catch (Exception ignore) {
            indexInfo = Parameters.nonNull(getRouteTable(schemaName, indexName, true), "Index not found.");
            fork = operation.fork(Any.wrap(parameters), indexInfo);
        }

        exec(indexInfo, operation, fork, context);

        return operation.reduce(fork);
    }

    private void exec(IndexInfo indexInfo, Operation operation, Operation.Fork fork, VectorContext context) {
        exec(indexInfo, operation, fork, retryTimes, context).ifPresent(e -> {
            if (!fork.isIgnoreError()) {
                throw new DingoClientException(-1, e);
            }
        });
    }

    private Optional<Throwable> exec(
        IndexInfo indexInfo,
        Operation operation,
        Operation.Fork fork,
        int retry,
        VectorContext vectorContext
    ) {
        if (retry <= 0) {
            return Optional.of(
                new DingoClientException(-1, "Exceeded the retry limit for performing " + operation.getClass()));
        }
        List<OperationContext> contexts = generateContext(indexInfo, fork, vectorContext);
        Optional<Throwable> error = Optional.empty();
        CountDownLatch countDownLatch = new CountDownLatch(contexts.size());
        contexts.forEach(context -> CompletableFuture
            .runAsync(() -> operation.exec(context), Executors.executor("exec-operator"))
            .thenApply(r -> Optional.<Throwable>empty())
            .exceptionally(Optional::of)
            .thenAccept(e -> {
                e.map(OperationUtils::getCause)
                    .ifPresent(__ -> log.error(__.getMessage(), __))
                    .map(err -> {
                        if (err instanceof DingoClientException.InvalidRouteTableException) {
                            IndexInfo newIndexInfo = getRouteTable(
                                indexInfo.schemaName.toUpperCase(),
                                indexInfo.indexName,
                                true
                            );
                            Operation.Fork newFork = operation.fork(context, newIndexInfo);
                            if (newFork == null) {
                                return exec(newIndexInfo, operation, newFork, 0, vectorContext).orNull();
                            }
                            return exec(newIndexInfo, operation, newFork, retry - 1, vectorContext).orNull();
                        } else {
                            return err;
                        }
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
        IndexInfo indexInfo,
        Operation.Fork fork,
        VectorContext vectorContext
    ) {
        int i = 0;
        List<OperationContext> contexts = new ArrayList<>(fork.getSubTasks().size());
        for (Operation.Task subTask : fork.getSubTasks()) {
            contexts.add(OperationContext.builder()
                .indexId(indexInfo.indexId)
                .regionId(subTask.getRegionId())
                .indexService(indexService)
                .seq(i++)
                .parameters(subTask.getParameters())
                .result(fork.getResultRef())
                .vectorContext(vectorContext)
                .build());
        }
        return contexts;
    }

    public synchronized boolean createIndex(String schema, String name, Index index) {
        if (index.getIsAutoIncrement() && index.getAutoIncrement() <= 0) {
            throw new DingoClientException("Auto-increment id only supports positive integers.");
        }
        MetaServiceClient metaService = getSubMetaService(schema);
        return metaService.createIndex(name, index);
    }

    public synchronized boolean updateIndex(String schema, String index, Index newIndex) {
        MetaServiceClient metaService = getSubMetaService(schema);
        return metaService.updateIndex(index, newIndex);
    }

    public boolean dropIndex(String schema, String indexName) {
        MetaServiceClient metaService = getSubMetaService(schema);
        routeTables.remove(schema.toUpperCase() + "." + indexName);
        return metaService.dropIndex(indexName);
    }

    public Index getIndex(String schema, String name) {
        MetaServiceClient metaService = getSubMetaService(schema);
        return metaService.getIndex(name);
    }

    public Index getIndex(String schema, String name, boolean forceRefresh) {
        return Parameters.nonNull(getRouteTable(schema.toUpperCase(), name, forceRefresh), "Index not found.").index;
    }

    public List<Index> getIndexes(String schema) {
        MetaServiceClient metaService = getSubMetaService(schema);
        return new ArrayList<>(metaService.getIndexes(mapping(metaService.id())).values());
    }

    public IndexMetrics getIndexMetrics(String schema, String index) {
        MetaServiceClient metaService = getSubMetaService(schema);
        return metaService.getIndexMetrics(index);
    }

    private MetaServiceClient getSubMetaService(String schemaName) {
        schemaName = schemaName.toUpperCase();
        return Parameters.nonNull(rootMetaService.getSubMetaService(schemaName), "Schema not found: " + schemaName);
    }

    private IndexInfo getRouteTable(String schemaName, String indexName, boolean forceRefresh) {
        return routeTables.compute(
            schemaName + "." + indexName,
            (k, v) -> cleanNull(forceRefresh ? null : v, () -> refreshRouteTable(schemaName, indexName))
        );
    }

    private IndexInfo refreshRouteTable(String schemaName, String indexName) {
        MetaServiceClient metaService = getSubMetaService(schemaName);
        schema.setIsKey(true);
        schema.setAllowNull(false);
        DingoCommonId indexId = Parameters.nonNull(metaService.getIndexId(indexName), "Index not found.");
        Index index = Parameters.nonNull(metaService.getIndex(indexName), "Index not found.");
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> parts =
            metaService.getIndexRangeDistribution(indexName);
        if (log.isDebugEnabled()) {
            for (RangeDistribution rangeDistribution : parts.values()) {
                log.info(">>>>>> refresh route table, regionId: {}, leader: {}, voters:{}, range: {} -- {}, region epoch: {} <<<<<<",
                    rangeDistribution.getId(),
                    rangeDistribution.getLeader(),
                    rangeDistribution.getVoters(),
                    rangeDistribution.getRange().getStartKey(),
                    rangeDistribution.getRange().getEndKey(),
                    rangeDistribution.getRegionEpoch());
            }
        }
        KeyValueCodec codec = new KeyValueCodec(
            new DingoKeyValueCodec(indexId.entityId(), Collections.singletonList(schema)), dingoType);

        return new IndexInfo(schemaName, indexName, indexId, index, codec, this.autoIncrementService, parts);
    }
}
