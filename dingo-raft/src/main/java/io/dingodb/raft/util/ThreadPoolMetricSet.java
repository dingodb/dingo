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

package io.dingodb.raft.util;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class ThreadPoolMetricSet implements MetricSet {
    private final ThreadPoolExecutor executor;

    public ThreadPoolMetricSet(ThreadPoolExecutor rpcExecutor) {
        super();
        this.executor = rpcExecutor;
    }

    /**
     * Return thread pool metrics
     * @return thread pool metrics map
     */
    @Override
    public Map<String, Metric> getMetrics() {
        final Map<String, Metric> gauges = new HashMap<>();
        gauges.put("pool-size", (Gauge<Integer>) executor::getPoolSize);
        gauges.put("queued", (Gauge<Integer>) executor.getQueue()::size);
        gauges.put("active", (Gauge<Integer>) executor::getActiveCount);
        gauges.put("completed", (Gauge<Long>) executor::getCompletedTaskCount);
        return gauges;
    }
}
