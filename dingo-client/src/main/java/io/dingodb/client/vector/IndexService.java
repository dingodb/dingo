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

package io.dingodb.client.vector;

import io.dingodb.client.VectorContext;
import io.dingodb.client.utils.OperationUtils;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Parameters;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.service.MetaService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.HnswParameter;
import io.dingodb.sdk.service.entity.meta.CreateIndexRequest;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.DropIndexRequest;
import io.dingodb.sdk.service.entity.meta.GenerateTableIdsRequest;
import io.dingodb.sdk.service.entity.meta.GetIndexByNameRequest;
import io.dingodb.sdk.service.entity.meta.GetIndexMetricsRequest;
import io.dingodb.sdk.service.entity.meta.GetIndexesRequest;
import io.dingodb.sdk.service.entity.meta.GetSchemaByNameRequest;
import io.dingodb.sdk.service.entity.meta.IndexDefinition;
import io.dingodb.sdk.service.entity.meta.IndexDefinitionWithId;
import io.dingodb.sdk.service.entity.meta.IndexMetrics;
import io.dingodb.sdk.service.entity.meta.Partition;
import io.dingodb.sdk.service.entity.meta.TableIdWithPartIds;
import io.dingodb.sdk.service.entity.meta.TableWithPartCount;
import io.dingodb.sdk.service.entity.meta.UpdateIndexRequest;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

@Slf4j
public class IndexService {

    private final IndexCache cache;
    private final MetaService metaService;
    private final int retryTimes;

    private final Set<Location> coordinators;

    public IndexService(String coordinatorSvr, int retryTimes) {
        this.coordinators = Services.parse(coordinatorSvr);
        this.metaService = Services.metaService(coordinators);
        this.retryTimes = retryTimes;
        this.cache = new IndexCache(retryTimes, coordinators);
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
        Index indexInfo = Parameters.nonNull(cache.getIndex(schemaName, indexName), "Index not found.");

        Operation.Fork fork = null;
        try {
            fork = operation.fork(Any.wrap(parameters), indexInfo);
        } catch (Exception ignore) {
            indexInfo = Parameters.nonNull(cache.getIndexNewly(schemaName, indexName), "Index not found.");
            fork = operation.fork(Any.wrap(parameters), indexInfo);
        }

        if (operation.stateful()) {
            exec(indexInfo, operation, fork, context);
        } else {
            int retry = retryTimes;
            while (retry-- > 0) {
                List<OperationContext> operationContexts = generateContext(indexInfo, fork, context);
                CompletableFuture<Void>[] futures = new CompletableFuture[operationContexts.size()];
                for (int i = 0; i < operationContexts.size(); i++) {
                    OperationContext operationContext = operationContexts.get(i);
                    futures[i] = CompletableFuture.runAsync(
                        () -> operation.exec(operationContext), Executors.executor("exec-op")
                    );
                }
                try {
                    CompletableFuture.allOf(futures).join();
                    break;
                } catch (Exception e) {
                    if (OperationUtils.getCause(e) instanceof DingoClientException.InvalidRouteTableException) {
                        indexInfo = Parameters.nonNull(cache.getIndexNewly(schemaName, indexName), "Index not found.");
                        fork = operation.fork(Any.wrap(parameters), indexInfo);
                        continue;
                    }
                    throw new RuntimeException(e);
                }
            }
        }

        return operation.reduce(fork);
    }

    private void exec(Index index, Operation operation, Operation.Fork fork, VectorContext context) {
        exec(index, operation, fork, retryTimes, context).ifPresent(e -> {
            if (!fork.isIgnoreError()) {
                throw new DingoClientException(-1, e);
            }
        });
    }

    private Optional<Throwable> exec(
        Index index,
        Operation operation,
        Operation.Fork fork,
        int retry,
        VectorContext vectorContext
    ) {
        if (retry <= 0) {
            return Optional.of(
                new DingoClientException(-1, "Exceeded the retry limit for performing " + operation.getClass()));
        }
        List<OperationContext> contexts = generateContext(index, fork, vectorContext);
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
                            Index newIndex = cache.getIndexNewly(
                                index.schemaName,
                                index.indexName
                            );
                            Operation.Fork newFork = operation.fork(context, newIndex);
                            if (newFork == null) {
                                return exec(newIndex, operation, newFork, 0, vectorContext).orNull();
                            }
                            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                            return exec(newIndex, operation, newFork, retry - 1, vectorContext).orNull();
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
        Index index,
        Operation.Fork fork,
        VectorContext vectorContext
    ) {
        int i = 0;
        long id = cache.tso();
        List<OperationContext> contexts = new ArrayList<>(fork.getSubTasks().size());
        DingoCommonId indexId = index.id;
        for (Operation.Task subTask : fork.getSubTasks()) {
            DingoCommonId regionId = subTask.getRegionId();
            contexts.add(OperationContext.builder()
                .requestId(id)
                .indexId(indexId)
                .regionId(regionId)
                .indexService(cache.getIndexService(regionId.getEntityId()))
                .seq(i++)
                .parameters(subTask.getParameters())
                .result(fork.getResultRef())
                .vectorContext(vectorContext)
                .build());
        }
        return contexts;
    }

    public synchronized boolean createIndex(String schema, IndexDefinition index) {
        if (index.getName().contains(".")) {
            throw new DingoClientException("Index name cannot has '.'");
        }
        if (index.isWithAutoIncrment() && index.getAutoIncrement() <= 0) {
            throw new DingoClientException("Auto-increment id only supports positive integers.");
        }
        schema = schema.toUpperCase();
        MetaService metaService = Services.metaService(coordinators);
        List<Partition> partitions = index.getIndexPartition().getPartitions();
        DingoCommonId schemaId = metaService
            .getSchemaByName(GetSchemaByNameRequest.builder().schemaName(schema).build()).getSchema().getId();
        TableIdWithPartIds tableIdWithPartIds = metaService.generateTableIds(GenerateTableIdsRequest.builder()
            .schemaId(schemaId)
            .count(TableWithPartCount.builder()
                .indexCount(1)
                .indexPartCount(Collections.singletonList(partitions.size()))
                .build())
            .build()
        ).getIds().get(0);
        for (int i = 0; i < partitions.size(); i++) {
            Partition partition = partitions.get(i);
            DingoCommonId partitionId = tableIdWithPartIds.getPartIds().get(i);
            partition.setId(partitionId);
            VectorKeyCodec.setEntityId(partitionId.getEntityId(), partition.getRange().getStartKey());
            partition.getRange().setEndKey(VectorKeyCodec.nextEntityKey(partitionId.getEntityId()));
        }
        metaService.createIndex(CreateIndexRequest.builder()
            .schemaId(schemaId)
            .indexDefinition(index)
            .indexId(tableIdWithPartIds.getTableId())
            .build()
        );
        return true;
    }

    public synchronized boolean dropIndex(String schema, String indexName) {
        MetaService metaService = Services.metaService(coordinators);
        schema = schema.toUpperCase();
        DingoCommonId schemaId = metaService.getSchemaByName(
            GetSchemaByNameRequest.builder().schemaName(schema).build()
        ).getSchema().getId();
        DingoCommonId indexId = metaService.getIndexByName(
            GetIndexByNameRequest.builder().schemaId(schemaId).indexName(indexName).build()
        ).getIndexDefinitionWithId().getIndexId();
        metaService.dropIndex(DropIndexRequest.builder().indexId(indexId).build());
        cache.invalidate(schema, indexName);
        return true;
    }

    public IndexDefinition getIndex(String schema, String name) {
        if (name.contains(".")) {
            throw new DingoClientException("Index name cannot has '.'");
        }
        MetaService metaService = Services.metaService(coordinators);
        schema = schema.toUpperCase();
        DingoCommonId schemaId = metaService.getSchemaByName(
            GetSchemaByNameRequest.builder().schemaName(schema).build()
        ).getSchema().getId();

        return metaService.getIndexByName(
            GetIndexByNameRequest.builder().schemaId(schemaId).indexName(name).build()
        ).getIndexDefinitionWithId().getIndexDefinition();
    }

    public IndexMetrics getIndexMetrics(String schema, String index) {
        schema = schema.toUpperCase();
        return metaService.getIndexMetrics(GetIndexMetricsRequest.builder()
            .indexId(cache.getIndex(schema, index).id)
            .build()
        ).getIndexMetrics().getIndexMetrics();
    }

    public IndexDefinition getIndex(String schema, String name, boolean forceRefresh) {
        schema = schema.toUpperCase();
        if (name.contains(".")) {
            throw new DingoClientException("Index name cannot has '.'");
        }
        return (forceRefresh ? cache.getIndex(schema, name) : cache.getIndexNewly(schema, name)).definition;
    }

    public List<IndexDefinition> getIndexes(String schema) {
        schema = schema.toUpperCase();
        MetaService metaService = Services.metaService(coordinators);
        DingoCommonId schemaId = metaService.getSchemaByName(
            GetSchemaByNameRequest.builder().schemaName(schema).build()
        ).getSchema().getId();
        return Optional.mapOrGet(
            metaService.getIndexes(GetIndexesRequest.builder().schemaId(schemaId).build()).getIndexDefinitionWithIds(),
            __ -> __.stream()
                .map(IndexDefinitionWithId::getIndexDefinition)
                .collect(Collectors.toList()),
            Collections::emptyList);
    }

    public boolean updateMaxElements(String schema, String index, int max) {
        schema = schema.toUpperCase();
        Index indexInfo = cache.getIndex(schema, index);
        IndexDefinition definition = indexInfo.definition;
        HnswParameter hnswParameter = (HnswParameter) definition.getIndexParameter().getVectorIndexParameter().getVectorIndexParameter();
        hnswParameter.setMaxElements(max);
        metaService.updateIndex(
            UpdateIndexRequest.builder().indexId(indexInfo.id).newIndexDefinition(definition).build()
        );
        return true;
    }
}
