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

package io.dingodb.common.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jmx.JmxReporter;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

public class DingoMetrics {
    private static final MetricRegistry metricRegistry = new MetricRegistry();

    static {
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
        jmxReporter.start();
    }

    public static Meter meter(@Nonnull final String name) {
        return metricRegistry.meter(name);
    }

    private static Timer timer(@Nonnull final String name) {
        return metricRegistry.timer(name);
    }

    public static Timer.Context getTimeContext(@Nonnull final String name) {
        return timer(name).time();
    }

    public static void latency(@Nonnull final String name, final long durationMs) {
        metricRegistry.timer(name).update(durationMs, TimeUnit.MILLISECONDS);
    }

    public static void histogram(@Nonnull final String name, final long size) {
        metricRegistry.histogram(name).update(size);
    }
}
