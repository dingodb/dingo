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

package io.dingodb.raft.rpc;

import io.dingodb.common.concurrent.Executors;
import io.dingodb.raft.Closure;
import io.dingodb.raft.Status;
import io.dingodb.raft.util.SystemPropertyUtil;
import io.dingodb.raft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class RpcUtils {
    private static final Logger LOG = LoggerFactory.getLogger(RpcUtils.class);

    /**
     * Default jraft closure executor pool minimum size, CPUs by default.
     */
    public static final int MIN_RPC_CLOSURE_EXECUTOR_POOL_SIZE = SystemPropertyUtil.getInt(
                                                                             "jraft.rpc.closure.threadpool.size.min",
                                                                             Utils.cpus());

    /**
     * Default jraft closure executor pool maximum size.
     */
    public static final int MAX_RPC_CLOSURE_EXECUTOR_POOL_SIZE = SystemPropertyUtil.getInt(
                                                                             "jraft.rpc.closure.threadpool.size.max",
                                                                             Math.max(100, Utils.cpus() * 5));

    /**
     * Run closure with OK status in thread pool.
     */
    public static Future<?> runClosureInThread(final Closure done) {
        if (done == null) {
            return null;
        }
        return runClosureInThread(done, Status.OK());
    }

    /**
     * Run a task in thread pool, returns the future object.
     */
    public static Future<?> runInThread(final Runnable runnable) {
        return Executors.submit("JRaft-Rpc-Closure-Executor", runnable);
    }

    /**
     * Run closure with status in thread pool.
     */
    public static Future<?> runClosureInThread(final Closure done, final Status status) {
        if (done == null) {
            return null;
        }

        return runInThread(() -> {
            try {
                done.run(status);
            } catch (final Throwable t) {
                LOG.error("Fail to run done closure.", t);
            }
        });
    }

    /**
     * Run closure with status in specified executor
     */
    public static void runClosureInExecutor(final Executor executor, final Closure done, final Status status) {
        if (done == null) {
            return;
        }

        if (executor == null) {
            runClosureInThread(done, status);
            return;
        }

        executor.execute(() -> {
            try {
                done.run(status);
            } catch (final Throwable t) {
                LOG.error("Fail to run done closure.", t);
            }
        });
    }

    private RpcUtils() {
    }
}
