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

package io.dingodb.raft.option;

import com.codahale.metrics.MetricRegistry;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class RpcOptions {
    /**
     * Rpc connect timeout in milliseconds
     * Default: 1000(1s)
     */
    private int rpcConnectTimeoutMs = 1000;

    /**
     * RPC request default timeout in milliseconds
     * Default: 5000(5s)
     */
    private int rpcDefaultTimeout = 5000;

    /**
     * Install snapshot RPC request default timeout in milliseconds
     * Default: 5 * 60 * 1000(5min)
     */
    private int rpcInstallSnapshotTimeout = 5 * 60 * 1000;

    /**
     * RPC process thread pool size
     * Default: 80
     */
    private int rpcProcessorThreadPoolSize = 80;

    /**
     * Whether to enable checksum for RPC.
     * Default: false
     */
    private boolean enableRpcChecksum = false;

    /**
     * Metric registry for RPC services, user should not use this field.
     */
    private MetricRegistry metricRegistry;

    public int getRpcConnectTimeoutMs() {
        return this.rpcConnectTimeoutMs;
    }

    public void setRpcConnectTimeoutMs(int rpcConnectTimeoutMs) {
        this.rpcConnectTimeoutMs = rpcConnectTimeoutMs;
    }

    public int getRpcDefaultTimeout() {
        return this.rpcDefaultTimeout;
    }

    public void setRpcDefaultTimeout(int rpcDefaultTimeout) {
        this.rpcDefaultTimeout = rpcDefaultTimeout;
    }

    public int getRpcInstallSnapshotTimeout() {
        return rpcInstallSnapshotTimeout;
    }

    public void setRpcInstallSnapshotTimeout(int rpcInstallSnapshotTimeout) {
        this.rpcInstallSnapshotTimeout = rpcInstallSnapshotTimeout;
    }

    public int getRpcProcessorThreadPoolSize() {
        return this.rpcProcessorThreadPoolSize;
    }

    public void setRpcProcessorThreadPoolSize(int rpcProcessorThreadPoolSize) {
        this.rpcProcessorThreadPoolSize = rpcProcessorThreadPoolSize;
    }

    public boolean isEnableRpcChecksum() {
        return enableRpcChecksum;
    }

    public void setEnableRpcChecksum(boolean enableRpcChecksum) {
        this.enableRpcChecksum = enableRpcChecksum;
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    public void setMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    @Override
    public String toString() {
        return "RpcOptions{" + "rpcConnectTimeoutMs=" + rpcConnectTimeoutMs + ", rpcDefaultTimeout="
               + rpcDefaultTimeout + ", rpcInstallSnapshotTimeout=" + rpcInstallSnapshotTimeout
               + ", rpcProcessorThreadPoolSize=" + rpcProcessorThreadPoolSize + ", enableRpcChecksum="
               + enableRpcChecksum + ", metricRegistry=" + metricRegistry + '}';
    }
}
