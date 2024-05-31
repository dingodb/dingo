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

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.UniformReservoir;
import com.codahale.metrics.jmx.JmxReporter;
import io.dingodb.common.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public final class DingoMetrics {
    public static final MetricRegistry metricRegistry = new MetricRegistry();

    public static AtomicLong activeTaskCount = new AtomicLong(0);
    private static final LoggerReporter slf4jReporter = LoggerReporter.forRegistry(metricRegistry).build();
    public static JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();

    public static Map<String, List<Long>> sqlCallLatencyMap = new ConcurrentHashMap<>();

    static {
        jmxReporter.start();
        slf4jReporter.start(60000, TimeUnit.MILLISECONDS);
        metricRegistry.register("forkCommonPool", new CachedGauge<Integer>(5, TimeUnit.MINUTES) {
            @Override
            protected Integer loadValue() {
                return ForkJoinPool.commonPool().getActiveThreadCount();
            }
        });
        metricRegistry.register("job_task_count", new CachedGauge<Integer>(5, TimeUnit.MINUTES) {
            @Override
            protected Integer loadValue() {
                return activeTaskCount.intValue();
            }
        });
        metricRegistry.register("globalSchedulerPool", new CachedGauge<Integer>(5, TimeUnit.MINUTES) {
            @Override
            protected Integer loadValue() {
                return Executors.getGlobalSchedulerPoolSize();
            }
        });
        metricRegistry.register("globalPool", new CachedGauge<Integer>(5, TimeUnit.MINUTES) {
            @Override
            protected Integer loadValue() {
                return Executors.getGlobalPoolSize();
            }
        });
        metricRegistry.register("lockPool", new CachedGauge<Integer>(5, TimeUnit.MINUTES) {
            @Override
            protected Integer loadValue() {
                return Executors.getLockPoolSize();
            }
        });
        metricRegistry.register("threadCount", new CachedGauge<Integer>(5, TimeUnit.MINUTES) {
            @Override
            protected Integer loadValue() {
                return Thread.activeCount();
            }
        });
        metricRegistry.register("heapUsage", new CachedGauge<Double>(5, TimeUnit.MINUTES) {
            @Override
            protected Double loadValue() {
                Runtime runtime = Runtime.getRuntime();
                long totalMemory = runtime.totalMemory();
                long freeMemory = runtime.freeMemory();
                return  (double) (totalMemory - freeMemory) / totalMemory * 100;
            }
        });
        metricRegistry.register("select-latency", new CachedGauge<Double>(5, TimeUnit.MINUTES) {
            @Override
            protected Double loadValue() {
                List<Long> durationList = sqlCallLatencyMap.get("select");
                if (durationList == null) {
                    return 0D;
                }
                double avg = durationList.stream().mapToInt(Long::intValue).average().orElse(0);
                sqlCallLatencyMap.remove("select");
                return avg;
            }
        });
        metricRegistry.register("delete-latency", new CachedGauge<Double>(5, TimeUnit.MINUTES) {
            @Override
            protected Double loadValue() {
                List<Long> durationList = sqlCallLatencyMap.get("delete");
                if (durationList == null) {
                    return 0D;
                }
                double avg = durationList.stream().mapToInt(Long::intValue).average().orElse(0);
                sqlCallLatencyMap.remove("delete");
                return avg;
            }
        });
        metricRegistry.register("update-latency", new CachedGauge<Double>(5, TimeUnit.MINUTES) {
            @Override
            protected Double loadValue() {
                List<Long> durationList = sqlCallLatencyMap.get("update");
                if (durationList == null) {
                    return 0D;
                }
                double avg = durationList.stream().mapToInt(Long::intValue).average().orElse(0);
                sqlCallLatencyMap.remove("update");
                return avg;
            }
        });
        metricRegistry.register("insert-latency", new CachedGauge<Double>(5, TimeUnit.MINUTES) {
            @Override
            protected Double loadValue() {
                List<Long> durationList = sqlCallLatencyMap.get("insert");
                if (durationList == null) {
                    return 0D;
                }
                double avg = durationList.stream().mapToInt(Long::intValue).average().orElse(0);
                sqlCallLatencyMap.remove("insert");
                return avg;
            }
        });

    }

    private DingoMetrics() {
    }

    public static Meter meter(final @NonNull String name) {
        return metricRegistry.meter(name);
    }

    public static Timer timer(final @NonNull String name) {
        return metricRegistry.timer(name, () -> new Timer(new UniformReservoir()));
    }

    public static Timer.Context getTimeContext(final @NonNull String name) {
        return timer(name).time();
    }

    public static void latency(final @NonNull String name, final long durationMs) {
        sqlCallLatencyMap.computeIfAbsent(name, key -> {
            List<Long> durationList = new ArrayList<>();
            durationList.add(durationMs);
            return durationList;
        });
        sqlCallLatencyMap.computeIfPresent(name, (k, v) -> {
            v.add(durationMs);
            return v;
        });
        metricRegistry.timer(name).update(durationMs, TimeUnit.MILLISECONDS);
    }

    public static void histogram(final @NonNull String name, final long size) {
        metricRegistry.histogram(name).update(size);
    }

    public static void startReporter() {
//        slf4jReporter.setMetricLogEnable(true);
    }

    public static void stopReporter() {
//        slf4jReporter.setMetricLogEnable(false);
    }
}
