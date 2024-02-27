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

package io.dingodb.common.concurrent;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.dingodb.common.util.DebugLog.error;

@Slf4j
public final class Executors {

    private static final String THREAD_NAME_FORMAT = "%s-%d";
    private static final String FREE_THREAD_NAME = "FREE";

    public static final String GLOBAL_NAME = "GLOBAL";
    public static final String GLOBAL_SCHEDULE_NAME = "GLOBAL_SCHEDULE";

    private static final ThreadPoolExecutor GLOBAL_POOL = new ThreadPoolBuilder()
        .name(GLOBAL_NAME)
        .coreThreads(0)
        .maximumThreads(Integer.MAX_VALUE)
        .keepAliveSeconds(TimeUnit.MINUTES.toSeconds(1))
        .workQueue(new SynchronousQueue<>())
        .daemon(true)
        .group(new ThreadGroup(GLOBAL_NAME))
        .build();

    private static final ScheduledThreadPoolExecutor GLOBAL_SCHEDULE_POOL = new ThreadPoolBuilder()
        .name(GLOBAL_SCHEDULE_NAME)
        .daemon(true)
        .coreThreads(1)
        .group(new ThreadGroup(GLOBAL_SCHEDULE_NAME))
        .buildSchedule();

    private static final Map<Thread, Context> contexts = new ConcurrentHashMap<>();

    private Executors() {
    }

    public static String threadName() {
        return Thread.currentThread().getName();
    }

    public static Context context() {
        return contexts.get(Thread.currentThread());
    }

    public static Executor executor(String name) {
        return command -> execute(name, command);
    }

    public static void execute(String name, Runnable command) {
        GLOBAL_POOL.execute(wrap(name, command));
    }

    public static void execute(String name, Runnable command, boolean ignoreError) {
        GLOBAL_POOL.execute(wrap(name, command, ignoreError));
    }

    public static ScheduledFuture<CompletableFuture<?>> scheduleAsync(
        String name, Runnable command, long delay, TimeUnit unit
    ) {
        return GLOBAL_SCHEDULE_POOL.schedule(() -> submit(name, command), delay, unit);
    }

    public static ScheduledFuture<CompletableFuture<?>> scheduleAsync(
        String name, Callable<?> command, long delay, TimeUnit unit
    ) {
        return GLOBAL_SCHEDULE_POOL.schedule(() -> submit(name, command), delay, unit);
    }

    public static ScheduledFuture<?> scheduleWithFixedDelay(
        String name, Runnable command, long initialDelay, long period, TimeUnit unit
    ) {
        return GLOBAL_SCHEDULE_POOL.scheduleWithFixedDelay(wrap(name, command), initialDelay, period, unit);
    }

    public static ScheduledFuture<?> scheduleWithFixedDelayAsync(
        String name, Runnable command, long initialDelay, long period, TimeUnit unit
    ) {
        return GLOBAL_SCHEDULE_POOL.scheduleWithFixedDelay(() -> execute(name, command), initialDelay, period, unit);
    }

    public static ScheduledFuture<?> scheduleAtFixedRate(
        String name, Runnable command, long initialDelay, long period, TimeUnit unit
    ) {
        return GLOBAL_SCHEDULE_POOL.scheduleAtFixedRate(wrap(name, command), initialDelay, period, unit);
    }

    public static ScheduledFuture<?> scheduleAtFixedRateAsync(
        String name, Runnable command, long initialDelay, long period, TimeUnit unit
    ) {
        return GLOBAL_SCHEDULE_POOL.scheduleAtFixedRate(() -> execute(name, command), initialDelay, period, unit);
    }

    public static <T> CompletableFuture<T> submit(String name, Callable<T> task) {
        CompletableFuture<T> future = new CompletableFuture<>();
        GLOBAL_POOL.execute(() -> {
            try {
                future.complete(wrap(name, task).call());
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    public static <T> CompletableFuture<T> submit(String name, Runnable task, T result) {
        CompletableFuture<T> future = new CompletableFuture<>();
        GLOBAL_POOL.execute(() -> {
            try {
                wrap(name, task).run();
                future.complete(result);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    public static CompletableFuture<Void> submit(String name, Runnable task) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        GLOBAL_POOL.execute(() -> {
            try {
                wrap(name, task).run();
                future.complete(null);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    private static <V> Callable<V> wrap(String name, Callable<V> callable) {
        return () -> call(name, callable, false);
    }

    private static <V> Callable<V> wrap(String name, Callable<V> callable, boolean ignoreError) {
        return () -> call(name, callable, ignoreError);
    }

    private static Runnable wrap(String name, Runnable runnable) {
        return () -> run(name, runnable, false);
    }

    private static Runnable wrap(String name, Runnable runnable, boolean ignoreError) {
        return () -> run(name, runnable, ignoreError);
    }

    private static <V> V call(String name, Callable<V> callable, boolean ignoreFalse) throws Exception {
        Thread thread = Thread.currentThread();
        contexts.put(thread, new Context());
        try {
            if (log.isTraceEnabled()) {
                log.trace("Call [{}] start, thread id [{}], set thread name.", name, thread.getId());
            }
            StringBuilder builder = new StringBuilder(name);
            builder.append("-").append(thread.getId());
            thread.setName(builder.toString());
            return callable.call();
        } catch (Throwable e) {
            if (ignoreFalse) {
                error(log, "Execute {} catch error.", name, e);
                return null;
            } else {
                log.error("Execute {} catch error.", name, e);
                throw e;
            }
        } finally {
            thread.setName(FREE_THREAD_NAME);
            if (log.isTraceEnabled()) {
                log.trace("Call [{}] finish, thread id [{}], reset thread name.", name, thread.getId());
            }
            contexts.remove(thread);
        }
    }

    private static void run(String name, Runnable runnable, boolean ignoreError) {
        Thread thread = Thread.currentThread();
        contexts.put(thread, new Context());
        try {
            if (log.isTraceEnabled()) {
                log.trace("Run [{}] start, thread id [{}], set thread name.", name, thread.getId());
            }
            StringBuilder builder = new StringBuilder(name);
            builder.append("-").append(thread.getId());
            thread.setName(builder.toString());
            runnable.run();
        } catch (Throwable e) {
            if (ignoreError) {
                error(log, "Execute {} catch error.", name, e);
            } else {
                log.error("Execute {} catch error.", name, e);
                throw e;
            }
        } finally {
            thread.setName(FREE_THREAD_NAME);
            if (log.isTraceEnabled()) {
                log.trace("Run [{}] finish, thread id [{}], reset thread name.", name, thread.getId());
            }
            contexts.put(thread, new Context());
        }
    }

}
