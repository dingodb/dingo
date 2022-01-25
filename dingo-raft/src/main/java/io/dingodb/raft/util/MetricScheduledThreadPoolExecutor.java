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

import com.codahale.metrics.Timer;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class MetricScheduledThreadPoolExecutor extends LogScheduledThreadPoolExecutor {
    public MetricScheduledThreadPoolExecutor(int corePoolSize, String name) {
        super(corePoolSize, name);
    }

    public MetricScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory, String name) {
        super(corePoolSize, threadFactory, name);
    }

    public MetricScheduledThreadPoolExecutor(int corePoolSize, RejectedExecutionHandler handler, String name) {
        super(corePoolSize, handler, name);
    }

    public MetricScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory,
                                             RejectedExecutionHandler handler, String name) {
        super(corePoolSize, threadFactory, handler, name);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        try {
            ThreadPoolMetricRegistry.timerThreadLocal() //
                .set(ThreadPoolMetricRegistry.metricRegistry().timer("scheduledThreadPool." + getName()).time());
        } catch (final Throwable ignored) {
            // ignored
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        try {
            final ThreadLocal<Timer.Context> tl = ThreadPoolMetricRegistry.timerThreadLocal();
            final Timer.Context ctx = tl.get();
            if (ctx != null) {
                ctx.stop();
                tl.remove();
            }
        } catch (final Throwable ignored) {
            // ignored
        }
    }
}
