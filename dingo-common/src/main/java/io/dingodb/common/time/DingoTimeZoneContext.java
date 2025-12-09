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

package io.dingodb.common.time;

import io.dingodb.expr.common.timezone.processor.DingoTimeZoneProcessor;
import lombok.extern.slf4j.Slf4j;

import java.time.ZoneId;
import java.util.TimeZone;

/**
 * Time zone context manager, uses ThreadLocal to store the time zone handler of the current thread.
 *
 * ThreadLocal provides access to the current session's time zone handler from anywhere.
 *
 * How to use:
 * 1. Call setProcessor() at the beginning of the session to set the processor
 * 2. Call getProcessor() where needed to obtain the processor
 * 3. Call clear() to clean up at the end of the session (important! Avoid memory leaks)
 */
@Slf4j
public class DingoTimeZoneContext {

    /**
     * Time zone processor, if null clears the current thread's processor.
     */
    // private static final ThreadLocal<DingoTimeZoneProcessor> PROCESSOR_HOLDER = new ThreadLocal<>();

    private static final InheritableThreadLocal<DingoTimeZoneProcessor> PROCESSOR_HOLDER =
        new InheritableThreadLocal<>();

    /**
     * ThreadLocal stores the time zone information of the current thread (used for debugging and logging).
     */
    // private static final ThreadLocal<TimeZone> TIMEZONE_HOLDER = new ThreadLocal<>();
    private static final InheritableThreadLocal<TimeZone> TIMEZONE_HOLDER = new InheritableThreadLocal<>();

    /**
     * Sets the time zone handler for the current thread.
     *
     * @param processor Time zone processor, if null clears the current thread's processor
     */
    public static void setProcessor(DingoTimeZoneProcessor processor) {
        if (processor == null) {
            clear();
        } else {
            PROCESSOR_HOLDER.set(processor);
            TimeZone timeZone = TimeZone.getTimeZone(processor.getOutputZone());
            TIMEZONE_HOLDER.set(timeZone);
            /* if (log.isDebugEnabled()) {
                log.debug("Set timezone processor for thread {}: {}",
                    Thread.currentThread().getName(), timeZone.getID());
            }*/
        }
    }

    /**
     * Create and set up a processor from TimeZone.
     *
     * @param timeZone time zone
     */
    public static void setTimeZone(TimeZone timeZone) {
        if (timeZone == null) {
            clear();
        } else {
            DingoTimeZoneProcessor processor = new DingoTimeZoneProcessor(timeZone.toZoneId());
            setProcessor(processor);
        }
    }

    /**
     * Create and set processor from ZoneId.
     *
     * @param zoneId zone id
     */
    public static void setZoneId(ZoneId zoneId) {
        if (zoneId == null) {
            clear();
        } else {
            DingoTimeZoneProcessor processor = new DingoTimeZoneProcessor(zoneId);
            setProcessor(processor);
        }
    }

    /**
     * Get the time zone handler of the current thread.
     *
     * @return Time zone handler, or null if not set
     */
    public static DingoTimeZoneProcessor getProcessor() {
        return getProcessorOrDefault();
    }

    /**
     * Get the time zone handler of the current thread. If it is not set, create one using the system default time zone.
     *
     * @return Time zone handler, will not return null
     */
    public static DingoTimeZoneProcessor getProcessorOrDefault() {
        DingoTimeZoneProcessor processor = PROCESSOR_HOLDER.get();
        if (processor == null) {
            TimeZone defaultTimeZone = TimeZone.getDefault();
            processor = new DingoTimeZoneProcessor(defaultTimeZone.toZoneId());
            if (log.isDebugEnabled()) {
                log.debug("No processor set for thread {}, using default timezone: {}",
                    Thread.currentThread().getName(), defaultTimeZone.getID());
            }
        }
        return processor;
    }

    /**
     * Get the time zone information of the current thread.
     *
     * @return Time zone object, or null if not set
     */
    public static TimeZone getTimeZone() {
        return TIMEZONE_HOLDER.get();
    }

    /**
     * Check if the current thread has set a processor.
     *
     * @return true if set, false otherwise
     */
    public static boolean hasProcessor() {
        return PROCESSOR_HOLDER.get() != null;
    }

    /**
     * Clear the time zone context of the current thread.
     *
     * Important: This method must be called at the end of the session to avoid memory leaks!
     * It is recommended to call it in a finally block or use try-with-resources mode
     */
    public static void clear() {
        PROCESSOR_HOLDER.remove();
        TIMEZONE_HOLDER.remove();
        if (log.isDebugEnabled()) {
            log.debug("Cleared timezone context for thread {}", Thread.currentThread().getName());
        }
    }

    /**
     * Automatically manage time zone context using try-with-resources pattern
     *
     * <pre>
     * try (DingoTimeZoneContext.Scope scope = DingoTimeZoneContext.withProcessor(processor)) {
     *     // Here processor is available
     *     // Automatic cleaning\
     * }
     * </pre>
     */
    public static Scope withProcessor(DingoTimeZoneProcessor processor) {
        return new Scope(processor);
    }

    /**
     * Automatically manage time zone context using try-with-resources pattern
     */
    public static Scope withTimeZone(TimeZone timeZone) {
        return new Scope(timeZone);
    }

    /**
     * Scope class for automatically managing time zone context
     */
    public static class Scope implements AutoCloseable {
        private final boolean hadProcessor;

        private Scope(DingoTimeZoneProcessor processor) {
            this.hadProcessor = hasProcessor();
            setProcessor(processor);
        }

        private Scope(TimeZone timeZone) {
            this.hadProcessor = hasProcessor();
            setTimeZone(timeZone);
        }

        @Override
        public void close() {
            if (!hadProcessor) {
                clear();
            }
        }
    }
}

