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
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.util.Optional;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.index.Index;
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
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static io.dingodb.sdk.common.utils.EntityConversion.mapping;

@Slf4j
public class IndexService {

    private final MetaServiceClient rootMetaService;
    private final IndexServiceClient indexService;
    private final AutoIncrementService autoIncrementService;
    private final int retryTimes;
    private final DingoSchema<Long> schema = new LongSchema(0);
    private final DingoType dingoType = new LongType(true);

    public IndexService(String coordinatorSvr, AutoIncrementService autoIncrementService, int retryTimes) {
        this.rootMetaService = new MetaServiceClient(coordinatorSvr);
        this.indexService = new IndexServiceClient(rootMetaService, retryTimes);
        this.autoIncrementService = autoIncrementService;
        this.retryTimes = retryTimes;
    }

    public <R> R exec(String schemaName, String indexName, Operation operation, Object parameters, VectorContext context) {
        MetaServiceClient metaService = getSubMetaService(schemaName);
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> parts =
            metaService.getIndexRangeDistribution(indexName);
        schema.setIsKey(true);
        DingoCommonId indexId = metaService.getIndexId(indexName);
        Index index = metaService.getIndex(indexName);
        KeyValueCodec codec = new KeyValueCodec(
            new DingoKeyValueCodec(indexId.entityId(), Collections.singletonList(schema)), dingoType);
        IndexInfo indexInfo = new IndexInfo(schemaName, indexName, indexId, index, codec, this.autoIncrementService, parts);
        Operation.Fork fork = operation.fork(Any.wrap(parameters), indexInfo);

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

    private Optional<Throwable> exec(IndexInfo indexInfo, Operation opeartion, Operation.Fork fork, int retry, VectorContext vectorContext) {
        if (retry <= 0) {
            return Optional.of(new DingoClientException(-1, "Exceeded the retry limit for performing " + opeartion.getClass()));
        }
        int i = 0;
        List<OperationContext> contexts = new ArrayList<>(fork.getSubTasks().size());
        for (Operation.Task subTask : fork.getSubTasks()) {
            contexts.add(OperationContext.builder()
                .indexId(indexInfo.indexId)
                .regionId(subTask.getRegionId())
                .indexService(indexService)
                .seq(i++)
                .parameters(subTask.getParameters())
                .result(Any.wrap(fork.result()))
                .vectorContext(vectorContext)
                .build());
        }

        Optional<Throwable> error = Optional.empty();
        CountDownLatch countDownLatch = new CountDownLatch(contexts.size());
        contexts.forEach(context -> CompletableFuture
            .runAsync(() -> opeartion.exec(context), Executors.executor("exec-operator"))
            .thenApply(r -> Optional.<Throwable>empty())
            .exceptionally(Optional::of)
            .thenAccept(e -> {
                e.ifPresent(error::ifAbsentSet);
                countDownLatch.countDown();
            }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.warn("Exec {} interrupted.", opeartion.getClass());
        }
        return error;
    }

    public synchronized boolean createIndex(String schema, String name, Index index) {
        MetaServiceClient metaService = getSubMetaService(schema);
        return metaService.createIndex(name, index);
    }

    public boolean dropIndex(String schema, String indexName) {
        MetaServiceClient metaService = getSubMetaService(schema);
        return metaService.dropIndex(indexName);
    }

    public Index getIndex(String schema, String name) {
        MetaServiceClient metaService = getSubMetaService(schema);
        return metaService.getIndex(name);
    }

    public List<Index> getIndexes(String schema) {
        MetaServiceClient metaService = getSubMetaService(schema);
        return new ArrayList<>(metaService.getIndexes(mapping(metaService.id())).values());
    }

    private MetaServiceClient getSubMetaService(String schemaName) {
        return Parameters.nonNull(rootMetaService.getSubMetaService(schemaName), "Schema not found: " + schemaName);
    }
}
