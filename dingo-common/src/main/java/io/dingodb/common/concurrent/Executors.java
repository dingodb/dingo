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

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
        .coreThreads(0)
        .group(new ThreadGroup(GLOBAL_SCHEDULE_NAME))
        .buildSchedule();

    private Executors() {
    }

    public static Executor executor(String name) {
        return command -> execute(name, command);
    }

    public static void execute(String name, Runnable command) {
        GLOBAL_POOL.execute(wrap(name, command));
    }

    public static void schedule(String name, Runnable command, long delay, TimeUnit unit) {
        GLOBAL_SCHEDULE_POOL.schedule(wrap(name, command), delay, unit);
    }

    public static <V> void schedule(String name, Callable<V> callable, long delay, TimeUnit unit) {
        GLOBAL_SCHEDULE_POOL.schedule(wrap(name, callable), delay, unit);
    }

    public static void schedule(String name, Runnable command, long initialDelay, long period, TimeUnit unit) {
        GLOBAL_SCHEDULE_POOL.scheduleWithFixedDelay(wrap(name, command), initialDelay, period, unit);
    }

    public static void scheduleAtFixedRate(
        String name, Runnable command, long initialDelay, long period, TimeUnit unit
    ) {
        GLOBAL_SCHEDULE_POOL.scheduleAtFixedRate(wrap(name, command), initialDelay, period, unit);
    }

    public static <T> Future<T> submit(String name, Callable<T> task) {
        return GLOBAL_POOL.submit(wrap(name, task));
    }

    public static <T> Future<T> submit(String name, Runnable task, T result) {
        return GLOBAL_POOL.submit(wrap(name, task), result);
    }

    public static Future<?> submit(String name, Runnable task) {
        return GLOBAL_POOL.submit(wrap(name, task));
    }

    private static <V> Callable<V> wrap(String name, Callable<V> callable) {
        return () -> call(name, callable);
    }

    private static Runnable wrap(String name, Runnable runnable) {
        return () -> run(name, runnable);
    }

    private static <V> V call(String name, Callable<V> callable) throws Exception {
        Thread thread = Thread.currentThread();
        try {
            if (log.isTraceEnabled()) {
                log.trace("Call [{}] start, thread id [{}], set thread name.", name, thread.getId());
            }
            thread.setName(String.format(THREAD_NAME_FORMAT, name, thread.getId()));
            return callable.call();
        } catch (Throwable e) {
            log.error("Execute {} catch error.", name, e);
            throw e;
        } finally {
            thread.setName(FREE_THREAD_NAME);
            if (log.isTraceEnabled()) {
                log.trace("Call [{}] finish, thread id [{}], reset thread name.", name, thread.getId());
            }
        }
    }

    private static void run(String name, Runnable runnable) {
        Thread thread = Thread.currentThread();
        try {
            if (log.isTraceEnabled()) {
                log.trace("Run [{}] start, thread id [{}], set thread name.", name, thread.getId());
            }
            thread.setName(String.format(THREAD_NAME_FORMAT, name, thread.getId()));
            runnable.run();
        } catch (Throwable e) {
            log.error("Execute {} catch error.", name, e);
            throw e;
        } finally {
            thread.setName(FREE_THREAD_NAME);
            if (log.isTraceEnabled()) {
                log.trace("Run [{}] finish, thread id [{}], reset thread name.", name, thread.getId());
            }
        }
    }

}
