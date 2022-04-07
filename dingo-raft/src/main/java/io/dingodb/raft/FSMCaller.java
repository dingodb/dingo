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

package io.dingodb.raft;

import io.dingodb.raft.closure.LoadSnapshotClosure;
import io.dingodb.raft.closure.SaveSnapshotClosure;
import io.dingodb.raft.entity.LeaderChangeContext;
import io.dingodb.raft.error.RaftException;
import io.dingodb.raft.option.FSMCallerOptions;
import io.dingodb.raft.util.Describer;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface FSMCaller extends Lifecycle<FSMCallerOptions>, Describer {
    /**
     * Listen on lastAppliedLogIndex update events.
     *
     * @author dennis
     */
    interface LastAppliedLogIndexListener {

        /**
         * Called when lastAppliedLogIndex updated.
         *
         * @param lastAppliedLogIndex the log index of last applied
         */
        void onApplied(final long lastAppliedLogIndex);
    }

    /**
     * Adds a LastAppliedLogIndexListener.
     */
    void addLastAppliedLogIndexListener(final LastAppliedLogIndexListener listener);

    /**
     * Called when log entry committed
     *
     * @param committedIndex committed log index
     */
    boolean onCommitted(final long committedIndex);

    /**
     * Called after loading snapshot.
     *
     * @param done callback
     */
    boolean onSnapshotLoad(final LoadSnapshotClosure done);

    /**
     * Called after saving snapshot.
     *
     * @param done callback
     */
    boolean onSnapshotSave(final SaveSnapshotClosure done);

    /**
     * Called when the leader stops.
     *
     * @param status status info
     */
    boolean onLeaderStop(final Status status);

    /**
     * Called when the leader starts.
     *
     * @param term current term
     */
    boolean onLeaderStart(final long term);

    /**
     * Called when start following a leader.
     *
     * @param ctx context of leader change
     */
    boolean onStartFollowing(final LeaderChangeContext ctx);

    /**
     * Called when stop following a leader.
     *
     * @param ctx context of leader change
     */
    boolean onStopFollowing(final LeaderChangeContext ctx);

    /**
     * Called when error happens.
     *
     * @param error error info
     */
    boolean onError(final RaftException error);

    /**
     * Returns the last log entry index to apply state machine.
     */
    long getLastAppliedIndex();

    /**
     * Called after shutdown to wait it terminates.
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    void join() throws InterruptedException;

    void doSnapshotSaveByAppliedIndex(SaveSnapshotClosure done);

    void reportFreezeSnapshotResult(boolean freezeResult, String errMsg);
}
