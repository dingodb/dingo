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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class LoggerReporter extends ScheduledReporter {
    private final LoggerReporter.LoggerProxy loggerProxy;
    private final Marker marker;
    private final String prefix;
    @Setter
    private volatile boolean metricLogEnable = true;

    public static LoggerReporter.Builder forRegistry(MetricRegistry registry) {
        return new LoggerReporter.Builder(registry);
    }

    protected LoggerReporter(MetricRegistry registry, LoggerReporter.LoggerProxy loggerProxy, Marker marker, String prefix, TimeUnit rateUnit, TimeUnit durationUnit, MetricFilter filter, ScheduledExecutorService executor, boolean shutdownExecutorOnStop) {
        super(registry, "logger-reporter", filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop);
        this.loggerProxy = loggerProxy;
        this.marker = marker;
        this.prefix = prefix;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        if (this.loggerProxy.isEnabled(this.marker) && metricLogEnable) {
            Iterator var6 = gauges.entrySet().iterator();

            Map.Entry entry;
            while(var6.hasNext()) {
                entry = (Map.Entry)var6.next();
                this.logGauge((String)entry.getKey(), (Gauge)entry.getValue());
            }

            var6 = counters.entrySet().iterator();

            while(var6.hasNext()) {
                entry = (Map.Entry)var6.next();
                this.logCounter((String)entry.getKey(), (Counter)entry.getValue());
            }

            var6 = histograms.entrySet().iterator();

            while(var6.hasNext()) {
                entry = (Map.Entry)var6.next();
                this.logHistogram((String)entry.getKey(), (Histogram)entry.getValue());
            }

            var6 = meters.entrySet().iterator();

            while(var6.hasNext()) {
                entry = (Map.Entry)var6.next();
                this.logMeter((String)entry.getKey(), (Meter)entry.getValue());
            }

            var6 = timers.entrySet().iterator();

            while(var6.hasNext()) {
                entry = (Map.Entry)var6.next();
                this.logTimer((String)entry.getKey(), (Timer)entry.getValue());
            }
            this.loggerProxy.log(this.marker, "------------------------------>");
        }
    }

    private void logTimer(String name, Timer timer) {
        Snapshot snapshot = timer.getSnapshot();
        this.loggerProxy.log(this.marker, "type={}, name={}, count={}, min={}, max={}, mean={}, median={}, p75={}, p95={}, p98={}, p99={}, p999={}", "TIMER", this.prefix(name), timer.getCount(), this.convertDuration((double)snapshot.getMin()), this.convertDuration((double)snapshot.getMax()), this.convertDuration(snapshot.getMean()), this.convertDuration(snapshot.getStdDev()), this.convertDuration(snapshot.getMedian()), this.convertDuration(snapshot.get75thPercentile()), this.convertDuration(snapshot.get95thPercentile()), this.convertDuration(snapshot.get98thPercentile()), this.convertDuration(snapshot.get99thPercentile()), this.convertDuration(snapshot.get999thPercentile()));
    }

    private void logMeter(String name, Meter meter) {
        this.loggerProxy.log(this.marker, "type={}, name={}, count={}, mean_rate={}, m1={}, m5={}, m15={}, rate_unit={}", "METER", this.prefix(name), meter.getCount(), this.convertRate(meter.getMeanRate()), this.convertRate(meter.getOneMinuteRate()), this.convertRate(meter.getFiveMinuteRate()), this.convertRate(meter.getFifteenMinuteRate()), this.getRateUnit());
    }

    private void logHistogram(String name, Histogram histogram) {
        Snapshot snapshot = histogram.getSnapshot();
        this.loggerProxy.log(this.marker, "type={}, name={}, count={}, min={}, max={}, mean={}, stddev={}, median={}, p75={}, p95={}, p98={}, p99={}, p999={}", "HISTOGRAM", this.prefix(name), histogram.getCount(), snapshot.getMin(), snapshot.getMax(), snapshot.getMean(), snapshot.getStdDev(), snapshot.getMedian(), snapshot.get75thPercentile(), snapshot.get95thPercentile(), snapshot.get98thPercentile(), snapshot.get99thPercentile(), snapshot.get999thPercentile());
    }

    private void logCounter(String name, Counter counter) {
        this.loggerProxy.log(this.marker, "type={}, name={}, count={}", "COUNTER", this.prefix(name), counter.getCount());
    }

    private void logGauge(String name, Gauge<?> gauge) {
        this.loggerProxy.log(this.marker, "type={}, name={}, value={}", "GAUGE", this.prefix(name), gauge.getValue());
    }

    protected String getRateUnit() {
        return "events/" + super.getRateUnit();
    }

    private String prefix(String... components) {
        return MetricRegistry.name(this.prefix, components);
    }

    private static class ErrorLoggerProxy extends LoggerReporter.LoggerProxy {
        public ErrorLoggerProxy(Logger logger) {
            super(logger);
        }

        public void log(Marker marker, String format, Object... arguments) {
            this.logger.error(marker, format, arguments);
        }

        public boolean isEnabled(Marker marker) {
            return this.logger.isErrorEnabled(marker);
        }
    }

    private static class WarnLoggerProxy extends LoggerReporter.LoggerProxy {
        public WarnLoggerProxy(Logger logger) {
            super(logger);
        }

        public void log(Marker marker, String format, Object... arguments) {
            this.logger.warn(marker, format, arguments);
        }

        public boolean isEnabled(Marker marker) {
            return this.logger.isWarnEnabled(marker);
        }
    }

    private static class InfoLoggerProxy extends LoggerReporter.LoggerProxy {
        public InfoLoggerProxy(Logger logger) {
            super(logger);
        }

        public void log(Marker marker, String format, Object... arguments) {
            this.logger.info(marker, format, arguments);
        }

        public boolean isEnabled(Marker marker) {
            return this.logger.isInfoEnabled(marker);
        }
    }

    private static class TraceLoggerProxy extends LoggerReporter.LoggerProxy {
        public TraceLoggerProxy(Logger logger) {
            super(logger);
        }

        public void log(Marker marker, String format, Object... arguments) {
            this.logger.trace(marker, format, arguments);
        }

        public boolean isEnabled(Marker marker) {
            return this.logger.isTraceEnabled(marker);
        }
    }

    private static class DebugLoggerProxy extends LoggerReporter.LoggerProxy {
        public DebugLoggerProxy(Logger logger) {
            super(logger);
        }

        public void log(Marker marker, String format, Object... arguments) {
            this.logger.debug(marker, format, arguments);
        }

        public boolean isEnabled(Marker marker) {
            return this.logger.isDebugEnabled(marker);
        }
    }

    abstract static class LoggerProxy {
        protected final Logger logger;

        public LoggerProxy(Logger logger) {
            this.logger = logger;
        }

        abstract void log(Marker var1, String var2, Object... var3);

        abstract boolean isEnabled(Marker var1);
    }

    public static class Builder {
        private final MetricRegistry registry;
        private Logger logger;
        private LoggerReporter.LoggingLevel loggingLevel;
        private Marker marker;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private ScheduledExecutorService executor;
        private boolean shutdownExecutorOnStop;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.logger = LoggerFactory.getLogger("metrics");
            this.marker = null;
            this.prefix = "";
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.loggingLevel = LoggerReporter.LoggingLevel.INFO;
            this.executor = null;
            this.shutdownExecutorOnStop = true;
        }

        public LoggerReporter.Builder shutdownExecutorOnStop(boolean shutdownExecutorOnStop) {
            this.shutdownExecutorOnStop = shutdownExecutorOnStop;
            return this;
        }

        public LoggerReporter.Builder scheduleOn(ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }

        public LoggerReporter.Builder outputTo(Logger logger) {
            this.logger = logger;
            return this;
        }

        public LoggerReporter.Builder markWith(Marker marker) {
            this.marker = marker;
            return this;
        }

        public LoggerReporter.Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public LoggerReporter.Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public LoggerReporter.Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public LoggerReporter.Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public LoggerReporter.Builder withLoggingLevel(LoggerReporter.LoggingLevel loggingLevel) {
            this.loggingLevel = loggingLevel;
            return this;
        }

        public LoggerReporter build() {
            Object loggerProxy;
            switch (this.loggingLevel) {
                case TRACE:
                    loggerProxy = new LoggerReporter.TraceLoggerProxy(this.logger);
                    break;
                case INFO:
                    loggerProxy = new LoggerReporter.InfoLoggerProxy(this.logger);
                    break;
                case WARN:
                    loggerProxy = new LoggerReporter.WarnLoggerProxy(this.logger);
                    break;
                case ERROR:
                    loggerProxy = new LoggerReporter.ErrorLoggerProxy(this.logger);
                    break;
                case DEBUG:
                default:
                    loggerProxy = new LoggerReporter.DebugLoggerProxy(this.logger);
            }

            return new LoggerReporter(this.registry, (LoggerReporter.LoggerProxy)loggerProxy, this.marker, this.prefix, this.rateUnit, this.durationUnit, this.filter, this.executor, this.shutdownExecutorOnStop);
        }
    }

    public static enum LoggingLevel {
        TRACE,
        DEBUG,
        INFO,
        WARN,
        ERROR;

        private LoggingLevel() {
        }
    }
}
