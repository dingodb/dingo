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

package io.dingodb.raft.core;

import io.dingodb.common.concurrent.Executors;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class TimerManager implements Scheduler {

    private final String name;

    public TimerManager(int workerNum) {
        this(workerNum, "JRaft-Node-ScheduleThreadPool");
    }

    public TimerManager(int workerNum, String name) {
        this.name = name;
    }

    @Override
    public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit) {
        return Executors.schedule(name, command, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, final long initialDelay, final long period,
                                                  final TimeUnit unit) {
        return Executors.scheduleAtFixedRate(name, command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, final long initialDelay, final long delay,
                                                     final TimeUnit unit) {
        return Executors.scheduleWithFixecDelay(name, command, initialDelay, delay, unit);
    }

    @Override
    public void shutdown() {
    }
}
