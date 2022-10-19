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
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ThreadFactoryBuilder {

    public static final String DEFAULT_NAME = "default-thread";


    private String name;
    private boolean daemon = false;
    private int priority = Thread.NORM_PRIORITY;
    private ThreadGroup group = Thread.currentThread().getThreadGroup();

    public ThreadFactoryBuilder name(String name) {
        this.name = name;
        return this;
    }

    public ThreadFactoryBuilder daemon(boolean daemon) {
        this.daemon = daemon;
        return this;
    }

    public ThreadFactoryBuilder priority(int priority) {
        this.priority = priority;
        return this;
    }

    public ThreadFactoryBuilder group(ThreadGroup group) {
        this.group = group;
        return this;
    }

    /**
     * build thread factory.
     */
    public ThreadFactory build() {
        return new ThreadFactory() {
            private final AtomicInteger index = new AtomicInteger(0);

            @Override
            public Thread newThread(@NonNull Runnable runnable) {
                String threadName = String.format("%s-thread-%d", name, this.index.incrementAndGet());
                Thread thread = new Thread(group, runnable, threadName);

                thread.setDaemon(daemon);
                thread.setPriority(priority);

                return thread;
            }
        };
    }
}
