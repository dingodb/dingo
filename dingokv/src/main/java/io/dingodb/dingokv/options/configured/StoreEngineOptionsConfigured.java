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

import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.util.Endpoint;
import io.dingodb.dingokv.options.HeartbeatOptions;
import io.dingodb.dingokv.options.MemoryDBOptions;
import io.dingodb.dingokv.options.RegionEngineOptions;
import io.dingodb.dingokv.options.RocksDBOptions;
import io.dingodb.dingokv.options.StoreEngineOptions;
import io.dingodb.dingokv.storage.StorageType;
import io.dingodb.dingokv.util.Configured;

import java.util.List;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class StoreEngineOptionsConfigured implements Configured<StoreEngineOptions> {
    private final StoreEngineOptions opts;

    public static StoreEngineOptionsConfigured newConfigured() {
        return new StoreEngineOptionsConfigured(new StoreEngineOptions());
    }

    public StoreEngineOptionsConfigured withStorageType(final StorageType storageType) {
        this.opts.setStorageType(storageType);
        return this;
    }

    public StoreEngineOptionsConfigured withRocksDBOptions(final RocksDBOptions rocksDBOptions) {
        this.opts.setRocksDBOptions(rocksDBOptions);
        return this;
    }

    public StoreEngineOptionsConfigured withMemoryDBOptions(final MemoryDBOptions memoryDBOptions) {
        this.opts.setMemoryDBOptions(memoryDBOptions);
        return this;
    }

    public StoreEngineOptionsConfigured withRaftDataPath(final String raftDataPath) {
        this.opts.setRaftDataPath(raftDataPath);
        return this;
    }

    public StoreEngineOptionsConfigured withServerAddress(final Endpoint serverAddress) {
        this.opts.setServerAddress(serverAddress);
        return this;
    }

    public StoreEngineOptionsConfigured withCommonNodeOptions(final NodeOptions nodeOptions) {
        this.opts.setCommonNodeOptions(nodeOptions);
        return this;
    }

    public StoreEngineOptionsConfigured withRegionEngineOptionsList(final List<RegionEngineOptions> regionEngineOptionsList) {
        this.opts.setRegionEngineOptionsList(regionEngineOptionsList);
        return this;
    }

    public StoreEngineOptionsConfigured withHeartbeatOptions(final HeartbeatOptions heartbeatOptions) {
        this.opts.setHeartbeatOptions(heartbeatOptions);
        return this;
    }

    public StoreEngineOptionsConfigured withUseSharedRpcExecutor(final boolean useSharedRpcExecutor) {
        this.opts.setUseSharedRpcExecutor(useSharedRpcExecutor);
        return this;
    }

    public StoreEngineOptionsConfigured withReadIndexCoreThreads(final int readIndexCoreThreads) {
        this.opts.setReadIndexCoreThreads(readIndexCoreThreads);
        return this;
    }

    public StoreEngineOptionsConfigured withLeaderStateTriggerCoreThreads(final int leaderStateTriggerCoreThreads) {
        this.opts.setLeaderStateTriggerCoreThreads(leaderStateTriggerCoreThreads);
        return this;
    }

    public StoreEngineOptionsConfigured withSnapshotCoreThreads(final int snapshotCoreThreads) {
        this.opts.setSnapshotCoreThreads(snapshotCoreThreads);
        return this;
    }

    public StoreEngineOptionsConfigured withCliRpcCoreThreads(final int cliRpcCoreThreads) {
        this.opts.setCliRpcCoreThreads(cliRpcCoreThreads);
        return this;
    }

    public StoreEngineOptionsConfigured withRaftRpcCoreThreads(final int raftRpcCoreThreads) {
        this.opts.setRaftRpcCoreThreads(raftRpcCoreThreads);
        return this;
    }

    public StoreEngineOptionsConfigured withKvRpcCoreThreads(final int kvRpcCoreThreads) {
        this.opts.setKvRpcCoreThreads(kvRpcCoreThreads);
        return this;
    }

    public StoreEngineOptionsConfigured withMetricsReportPeriod(final long metricsReportPeriod) {
        this.opts.setMetricsReportPeriod(metricsReportPeriod);
        return this;
    }

    public StoreEngineOptionsConfigured withLeastKeysOnSplit(final long leastKeysOnSplit) {
        this.opts.setLeastKeysOnSplit(leastKeysOnSplit);
        return this;
    }

    @Override
    public StoreEngineOptions config() {
        return this.opts;
    }

    private StoreEngineOptionsConfigured(StoreEngineOptions opts) {
        this.opts = opts;
    }
}
