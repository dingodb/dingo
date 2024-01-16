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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.dingodb.common.util.Parameters;
import io.dingodb.sdk.service.AutoIncrementService;
import io.dingodb.sdk.service.IndexService;
import io.dingodb.sdk.service.MetaService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.GetIndexByNameRequest;
import io.dingodb.sdk.service.entity.meta.GetIndexRangeRequest;
import io.dingodb.sdk.service.entity.meta.GetSchemaByNameRequest;
import io.dingodb.sdk.service.entity.meta.IndexDefinitionWithId;
import io.dingodb.sdk.service.entity.meta.RangeDistribution;
import io.dingodb.store.proxy.service.TsoService;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class IndexCache {

    private final int retry;
    private final Set<Location> coordinators;
    private final MetaService metaService;
    private final AutoIncrementService autoIncrementService;
    private final TsoService tsoService;

    private final LoadingCache<String, LoadingCache<String, Index>> indexCache = CacheBuilder
        .newBuilder()
        .expireAfterAccess(60, TimeUnit.MINUTES)
        .expireAfterWrite(60, TimeUnit.MINUTES)
        .maximumSize(8)
        .build(new CacheLoader<String, LoadingCache<String, Index>>() {
            @Override
            public LoadingCache<String, Index> load(String schema) throws Exception {
                DingoCommonId schemaId = Parameters.nonNull(
                    metaService.getSchemaByName(
                        GetSchemaByNameRequest.builder().schemaName(schema).build()
                    ).getSchema(),
                    "Schema " + schema + " not found."
                ).getId();
                return CacheBuilder.newBuilder()
                    .expireAfterAccess(10, TimeUnit.MINUTES)
                    .expireAfterWrite(10, TimeUnit.MINUTES)
                    .maximumSize(64)
                    .build(new CacheLoader<String, Index>() {
                        @Override
                        public Index load(String name) throws Exception {
                            IndexDefinitionWithId indexWithId = metaService.getIndexByName(
                                GetIndexByNameRequest.builder().schemaId(schemaId).indexName(name).build()
                            ).getIndexDefinitionWithId();
                            Parameters.nonNull(indexWithId, "Index " + name + " not found.");
                            List<RangeDistribution> distributions = metaService.getIndexRange(
                                GetIndexRangeRequest.builder().indexId(indexWithId.getIndexId()).build()
                            ).getIndexRange().getRangeDistribution();
                            distributions.forEach($ -> {
                                $.setExt$(Services.indexRegionService(coordinators, $.getId().getEntityId(), retry));
                            });
                            Partitions partitions = new Partitions(
                                indexWithId.getIndexDefinition().getIndexPartition().getStrategy(), distributions
                            );
                            return new Index(
                                indexWithId.getIndexId(),
                                indexWithId.getIndexDefinition(),
                                distributions,
                                partitions,
                                schema,
                                name,
                                autoIncrementService
                            );
                        }
                    });
            }
        });

    public IndexCache(int retry, Set<Location> coordinators) {
        this.retry = retry;
        this.coordinators = coordinators;
        this.metaService = Services.metaService(coordinators);
        this.autoIncrementService = new AutoIncrementService(coordinators);
        this.tsoService = new TsoService(coordinators);
    }

    public long tso() {
        return tsoService.tso();
    }

    @SneakyThrows
    public Index getIndex(String schema, String name) {
        return indexCache.get(schema).get(name);
    }

    @SneakyThrows
    public Index getIndexNewly(String schema, String name) {
        LoadingCache<String, Index> cache = indexCache.get(schema);
        cache.invalidate(name);
        return cache.get(name);
    }

    @SneakyThrows
    public void invalidate(String schema, String name) {
        indexCache.get(schema).invalidate(name);
    }

    public IndexService getIndexService(long regionId) {
        return Services.indexRegionService(coordinators, regionId, retry);
    }

}
