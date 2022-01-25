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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class ThreadPoolMetricRegistry {
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final ThreadLocal<Timer.Context> timerThreadLocal = new ThreadLocal<>();

    /**
     * Return the global registry of metric instances.
     */
    public static MetricRegistry metricRegistry() {
        return metricRegistry;
    }

    public static ThreadLocal<Timer.Context> timerThreadLocal() {
        return timerThreadLocal;
    }
}
