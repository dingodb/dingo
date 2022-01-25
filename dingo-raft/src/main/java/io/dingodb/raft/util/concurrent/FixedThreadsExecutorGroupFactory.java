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

import java.util.concurrent.ExecutorService;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface FixedThreadsExecutorGroupFactory {
    FixedThreadsExecutorGroup newExecutorGroup(final int nThreads, final String poolName,
                                               final int maxPendingTasksPerThread);

    FixedThreadsExecutorGroup newExecutorGroup(final int nThreads, final String poolName,
                                               final int maxPendingTasksPerThread, final boolean useMpscQueue);

    FixedThreadsExecutorGroup newExecutorGroup(final SingleThreadExecutor[] children);

    FixedThreadsExecutorGroup newExecutorGroup(final SingleThreadExecutor[] children,
                                               final ExecutorChooserFactory.ExecutorChooser chooser);

    FixedThreadsExecutorGroup newExecutorGroup(final ExecutorService[] children);

    FixedThreadsExecutorGroup newExecutorGroup(final ExecutorService[] children,
                                               final ExecutorChooserFactory.ExecutorChooser chooser);

}
