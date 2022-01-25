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

package io.dingodb.raft.closure;

import io.dingodb.raft.Closure;

import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@ThreadSafe
public interface ClosureQueue {
    /**
     * Clear all closure in queue.
     */
    void clear();

    /**
     * Reset the first index in queue.
     *
     * @param firstIndex the first index of queue
     */
    void resetFirstIndex(final long firstIndex);

    /**
     * Append a new closure into queue.
     *
     * @param closure the closure to append
     */
    void appendPendingClosure(final Closure closure);

    /**
     * Pop closure from queue until index(inclusion), returns the first
     * popped out index, returns -1 when out of range, returns index+1
     * when not found.
     *
     * @param endIndex     the index of queue
     * @param closures     closure list
     * @return returns the first popped out index, returns -1 when out
     * of range, returns index+1
     * when not found.
     */
    long popClosureUntil(final long endIndex, final List<Closure> closures);

    /**
     * Pop closure from queue until index(inclusion), returns the first
     * popped out index, returns -1 when out of range, returns index+1
     * when not found.
     *
     * @param endIndex     the index of queue
     * @param closures     closure list
     * @param taskClosures task closure list
     * @return returns the first popped out index, returns -1 when out
     * of range, returns index+1
     * when not found.
     */
    long popClosureUntil(final long endIndex, final List<Closure> closures, final List<TaskClosure> taskClosures);
}
