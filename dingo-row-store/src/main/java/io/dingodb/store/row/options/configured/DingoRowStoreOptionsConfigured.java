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

package io.dingodb.store.row.options.configured;

import io.dingodb.store.row.options.BatchingOptions;
import io.dingodb.store.row.options.PlacementDriverOptions;
import io.dingodb.store.row.options.DingoRowStoreOptions;
import io.dingodb.store.row.options.RpcOptions;
import io.dingodb.store.row.options.StoreEngineOptions;
import io.dingodb.store.row.util.Configured;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class DingoRowStoreOptionsConfigured implements Configured<DingoRowStoreOptions> {
    private final DingoRowStoreOptions opts;

    public static DingoRowStoreOptionsConfigured newConfigured() {
        return new DingoRowStoreOptionsConfigured(new DingoRowStoreOptions());
    }

    public DingoRowStoreOptionsConfigured withClusterId(final long clusterId) {
        this.opts.setClusterId(clusterId);
        return this;
    }

    public DingoRowStoreOptionsConfigured withClusterName(final String clusterName) {
        this.opts.setClusterName(clusterName);
        return this;
    }

    public DingoRowStoreOptionsConfigured withPlacementDriverOptions(final PlacementDriverOptions placementDriverOptions) {
        this.opts.setPlacementDriverOptions(placementDriverOptions);
        return this;
    }

    public DingoRowStoreOptionsConfigured withStoreEngineOptions(final StoreEngineOptions storeEngineOptions) {
        this.opts.setStoreEngineOptions(storeEngineOptions);
        return this;
    }

    public DingoRowStoreOptionsConfigured withInitialServerList(final String initialServerList) {
        this.opts.setInitialServerList(initialServerList);
        return this;
    }

    public DingoRowStoreOptionsConfigured withOnlyLeaderRead(final boolean onlyLeaderRead) {
        this.opts.setOnlyLeaderRead(onlyLeaderRead);
        return this;
    }

    public DingoRowStoreOptionsConfigured withRpcOptions(final RpcOptions rpcOptions) {
        this.opts.setRpcOptions(rpcOptions);
        return this;
    }

    public DingoRowStoreOptionsConfigured withFailoverRetries(final int failoverRetries) {
        this.opts.setFailoverRetries(failoverRetries);
        return this;
    }

    public DingoRowStoreOptionsConfigured withFutureTimeoutMillis(final long futureTimeoutMillis) {
        this.opts.setFutureTimeoutMillis(futureTimeoutMillis);
        return this;
    }

    public DingoRowStoreOptionsConfigured withUseParallelKVExecutor(final boolean useParallelKVExecutor) {
        this.opts.setUseParallelKVExecutor(useParallelKVExecutor);
        return this;
    }

    public DingoRowStoreOptionsConfigured withBatchingOptions(final BatchingOptions batchingOptions) {
        this.opts.setBatchingOptions(batchingOptions);
        return this;
    }

    public DingoRowStoreOptionsConfigured withUseParallelCompress(final boolean useParallelCompress) {
        this.opts.setUseParallelCompress(useParallelCompress);
        return this;
    }

    public DingoRowStoreOptionsConfigured withCompressThreads(final int compressThreads) {
        this.opts.setCompressThreads(compressThreads);
        return this;
    }

    public DingoRowStoreOptionsConfigured withDeCompressThreads(final int deCompressThreads) {
        this.opts.setDeCompressThreads(deCompressThreads);
        return this;
    }

    @Override
    public DingoRowStoreOptions config() {
        return this.opts;
    }

    private DingoRowStoreOptionsConfigured(DingoRowStoreOptions opts) {
        this.opts = opts;
    }
}
