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

package io.dingodb.raft.util.timer;

import io.dingodb.common.concurrent.Executors;
import io.dingodb.raft.util.Requires;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class DefaultTimer implements Timer {

    private final String name;

    public DefaultTimer(int workerNum, String name) {
        this.name = name;
    }

    @Override
    public Timeout newTimeout(final TimerTask task, final long delay, final TimeUnit unit) {
        Requires.requireNonNull(task, "task");
        Requires.requireNonNull(unit, "unit");

        final TimeoutTask timeoutTask = new TimeoutTask(task);
        final ScheduledFuture<?> future = Executors.schedule(name, new TimeoutTask(task), delay, unit);
        timeoutTask.setFuture(future);
        return timeoutTask.getTimeout();
    }

    @Override
    public Set<Timeout> stop() {
        return Collections.emptySet();
    }

    private class TimeoutTask implements Runnable {
        private final TimerTask task;
        private final Timeout timeout;
        private volatile ScheduledFuture<?> future;

        private TimeoutTask(TimerTask task) {
            this.task = task;
            this.timeout = new Timeout() {

                @Override
                public Timer timer() {
                    return DefaultTimer.this;
                }

                @Override
                public TimerTask task() {
                    return task;
                }

                @Override
                public boolean isExpired() {
                    return false; // never use
                }

                @Override
                public boolean isCancelled() {
                    final ScheduledFuture<?> f = future;
                    return f != null && f.isCancelled();
                }

                @Override
                public boolean cancel() {
                    final ScheduledFuture<?> f = future;
                    return f != null && f.cancel(false);
                }
            };
        }

        public Timeout getTimeout() {
            return timeout;
        }

        public ScheduledFuture<?> getFuture() {
            return future;
        }

        public void setFuture(ScheduledFuture<?> future) {
            this.future = future;
        }

        @Override
        public void run() {
            try {
                this.task.run(this.timeout);
            } catch (final Throwable ignored) {
                // never get here
            }
        }
    }
}
