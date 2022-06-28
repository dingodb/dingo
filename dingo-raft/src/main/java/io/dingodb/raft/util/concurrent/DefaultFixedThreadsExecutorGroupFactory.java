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

import io.dingodb.raft.util.Requires;
import io.dingodb.raft.util.Utils;

import java.util.concurrent.ExecutorService;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class DefaultFixedThreadsExecutorGroupFactory implements FixedThreadsExecutorGroupFactory {
    public static final DefaultFixedThreadsExecutorGroupFactory INSTANCE = new DefaultFixedThreadsExecutorGroupFactory();

    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final int nThreads, final String poolName,
                                                      final int maxPendingTasksPerThread) {
        return newExecutorGroup(nThreads, poolName, maxPendingTasksPerThread, false);
    }

    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final int nThreads, final String poolName,
                                                      final int maxPendingTasksPerThread, final boolean useMpscQueue) {
        Requires.requireTrue(nThreads > 0, "nThreads must > 0");
        final boolean mpsc = useMpscQueue && Utils.USE_MPSC_SINGLE_THREAD_EXECUTOR;
        final SingleThreadExecutor[] children = new SingleThreadExecutor[nThreads];
        for (int i = 0; i < nThreads; i++) {
            if (mpsc) {
                children[i] = new MpscSingleThreadExecutor(maxPendingTasksPerThread, poolName);
            } else {
                children[i] = new DefaultSingleThreadExecutor(poolName, maxPendingTasksPerThread);
            }
        }
        return new DefaultFixedThreadsExecutorGroup(children);
    }

    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final SingleThreadExecutor[] children) {
        return new DefaultFixedThreadsExecutorGroup(children);
    }

    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final SingleThreadExecutor[] children,
                                                      final ExecutorChooserFactory.ExecutorChooser chooser) {
        return new DefaultFixedThreadsExecutorGroup(children, chooser);
    }

    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final ExecutorService[] children) {
        return new DefaultFixedThreadsExecutorGroup(children);
    }

    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final ExecutorService[] children,
                                                      final ExecutorChooserFactory.ExecutorChooser chooser) {
        return new DefaultFixedThreadsExecutorGroup(children, chooser);
    }

    private DefaultFixedThreadsExecutorGroupFactory() {
    }
}
