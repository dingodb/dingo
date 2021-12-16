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

package io.dingodb.dingokv.options.configured;

import io.dingodb.dingokv.options.BatchingOptions;
import io.dingodb.dingokv.options.PlacementDriverOptions;
import io.dingodb.dingokv.options.DingoKVStoreOptions;
import io.dingodb.dingokv.options.RpcOptions;
import io.dingodb.dingokv.options.StoreEngineOptions;
import io.dingodb.dingokv.util.Configured;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class DingoKVStoreOptionsConfigured implements Configured<DingoKVStoreOptions> {
    private final DingoKVStoreOptions opts;

    public static DingoKVStoreOptionsConfigured newConfigured() {
        return new DingoKVStoreOptionsConfigured(new DingoKVStoreOptions());
    }

    public DingoKVStoreOptionsConfigured withClusterId(final long clusterId) {
        this.opts.setClusterId(clusterId);
        return this;
    }

    public DingoKVStoreOptionsConfigured withClusterName(final String clusterName) {
        this.opts.setClusterName(clusterName);
        return this;
    }

    public DingoKVStoreOptionsConfigured withPlacementDriverOptions(final PlacementDriverOptions placementDriverOptions) {
        this.opts.setPlacementDriverOptions(placementDriverOptions);
        return this;
    }

    public DingoKVStoreOptionsConfigured withStoreEngineOptions(final StoreEngineOptions storeEngineOptions) {
        this.opts.setStoreEngineOptions(storeEngineOptions);
        return this;
    }

    public DingoKVStoreOptionsConfigured withInitialServerList(final String initialServerList) {
        this.opts.setInitialServerList(initialServerList);
        return this;
    }

    public DingoKVStoreOptionsConfigured withOnlyLeaderRead(final boolean onlyLeaderRead) {
        this.opts.setOnlyLeaderRead(onlyLeaderRead);
        return this;
    }

    public DingoKVStoreOptionsConfigured withRpcOptions(final RpcOptions rpcOptions) {
        this.opts.setRpcOptions(rpcOptions);
        return this;
    }

    public DingoKVStoreOptionsConfigured withFailoverRetries(final int failoverRetries) {
        this.opts.setFailoverRetries(failoverRetries);
        return this;
    }

    public DingoKVStoreOptionsConfigured withFutureTimeoutMillis(final long futureTimeoutMillis) {
        this.opts.setFutureTimeoutMillis(futureTimeoutMillis);
        return this;
    }

    public DingoKVStoreOptionsConfigured withUseParallelKVExecutor(final boolean useParallelKVExecutor) {
        this.opts.setUseParallelKVExecutor(useParallelKVExecutor);
        return this;
    }

    public DingoKVStoreOptionsConfigured withBatchingOptions(final BatchingOptions batchingOptions) {
        this.opts.setBatchingOptions(batchingOptions);
        return this;
    }

    public DingoKVStoreOptionsConfigured withUseParallelCompress(final boolean useParallelCompress) {
        this.opts.setUseParallelCompress(useParallelCompress);
        return this;
    }

    public DingoKVStoreOptionsConfigured withCompressThreads(final int compressThreads) {
        this.opts.setCompressThreads(compressThreads);
        return this;
    }

    public DingoKVStoreOptionsConfigured withDeCompressThreads(final int deCompressThreads) {
        this.opts.setDeCompressThreads(deCompressThreads);
        return this;
    }

    @Override
    public DingoKVStoreOptions config() {
        return this.opts;
    }

    private DingoKVStoreOptionsConfigured(DingoKVStoreOptions opts) {
        this.opts = opts;
    }
}
