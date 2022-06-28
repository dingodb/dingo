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

package io.dingodb.raft.util.concurrent;

import io.dingodb.common.concurrent.Executors;
import io.dingodb.raft.util.ExecutorServiceHelper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class DefaultSingleThreadExecutor
    implements SingleThreadExecutor {

    private final SingleThreadExecutor singleThreadExecutor;

    /**
     * Anti-gentleman is not against villains, we believe that you are
     * providing a single-thread executor.
     *
     * @param singleThreadExecutorService a {@link ExecutorService} instance
     */
    public DefaultSingleThreadExecutor(ExecutorService singleThreadExecutorService) {
        this.singleThreadExecutor = wrapSingleThreadExecutor(singleThreadExecutorService);
    }

    public DefaultSingleThreadExecutor(String poolName, int maxPendingTasks) {
        this.singleThreadExecutor = createSingleThreadExecutor(poolName, maxPendingTasks);
    }

    @Override
    public void execute(final Runnable task) {
        this.singleThreadExecutor.execute(task);
    }

    @Override
    public boolean shutdownGracefully() {
        return this.singleThreadExecutor.shutdownGracefully();
    }

    @Override
    public boolean shutdownGracefully(final long timeout, final TimeUnit unit) {
        return this.singleThreadExecutor.shutdownGracefully(timeout, unit);
    }

    private static SingleThreadExecutor wrapSingleThreadExecutor(final ExecutorService executor) {
        if (executor instanceof SingleThreadExecutor) {
            return (SingleThreadExecutor) executor;
        } else {
            return new SingleThreadExecutor() {

                @Override
                public boolean shutdownGracefully() {
                    return ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
                }

                @Override
                public boolean shutdownGracefully(final long timeout, final TimeUnit unit) {
                    return ExecutorServiceHelper.shutdownAndAwaitTermination(executor, unit.toMillis(timeout));
                }

                @Override
                public void execute(final Runnable command) {
                    executor.execute(command);
                }
            };
        }
    }

    private static SingleThreadExecutor createSingleThreadExecutor(final String poolName, final int maxPendingTasks) {

        return new SingleThreadExecutor() {

            @Override
            public boolean shutdownGracefully() {
                return true;
            }

            @Override
            public boolean shutdownGracefully(final long timeout, final TimeUnit unit) {
                return true;
            }

            @Override
            public void execute(final Runnable command) {
                Executors.execute(poolName, command);
            }
        };
    }
}
