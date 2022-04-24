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

package io.dingodb.raft.kv.storage;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.dingodb.raft.kv.storage.PhaseCommitAck.Phase.ALL;
import static io.dingodb.raft.kv.storage.PhaseCommitAck.Phase.FIRST;
import static io.dingodb.raft.kv.storage.PhaseCommitAck.Phase.LOG;
import static io.dingodb.raft.kv.storage.PhaseCommitAck.Phase.MAJOR;
import static io.dingodb.raft.kv.storage.PhaseCommitAck.Phase.NONE;

public class PhaseCommitAck {

    public enum Phase {
        NONE,
        LOG,
        FIRST,
        MAJOR,
        ALL
    }

    private final AtomicBoolean log = new AtomicBoolean(false);
    private final AtomicBoolean first = new AtomicBoolean(false);
    private final AtomicBoolean major = new AtomicBoolean(false);
    private final AtomicBoolean all = new AtomicBoolean(false);

    public PhaseCommitAck complete() {
        if (log.compareAndSet(false, true)) {
            return this;
        }
        if (first.compareAndSet(false, true)) {
            return this;
        }
        if (major.compareAndSet(false, true)) {
            return this;
        }
        all.compareAndSet(false, true);
        return this;
    }

    public Phase current() {
        if (all.compareAndSet(true, true)) {
            return ALL;
        }
        if (major.compareAndSet(true, true)) {
            return MAJOR;
        }
        if (first.compareAndSet(true, true)) {
            return FIRST;
        }
        if (log.compareAndSet(true, true)) {
            return LOG;
        }
        return NONE;
    }

    public boolean log() {
        return log.compareAndSet(true, true);
    }

    public boolean first() {
        return first.compareAndSet(true, true);
    }

    public boolean major() {
        return major.compareAndSet(true, true);
    }

    public boolean all() {
        return all.compareAndSet(true, true);
    }

}
