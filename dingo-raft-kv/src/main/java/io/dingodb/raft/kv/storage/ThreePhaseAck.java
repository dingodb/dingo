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

import static io.dingodb.raft.kv.storage.ThreePhaseAck.Phase.FIRST;
import static io.dingodb.raft.kv.storage.ThreePhaseAck.Phase.LOG;
import static io.dingodb.raft.kv.storage.ThreePhaseAck.Phase.MAJOR;
import static io.dingodb.raft.kv.storage.ThreePhaseAck.Phase.NONE;

public class ThreePhaseAck {

    public enum Phase {
        NONE,
        LOG,
        FIRST,
        MAJOR
    }

    private final AtomicBoolean log = new AtomicBoolean(false);
    private final AtomicBoolean first = new AtomicBoolean(false);
    private final AtomicBoolean major = new AtomicBoolean(false);

    public void complete() {
        if (log.compareAndSet(false, true)) {
            return;
        }
        if (first.compareAndSet(false, true)) {
            return;
        }
        if (major.compareAndSet(false, true)) {
            return;
        }
        return;
    }

    public Phase current() {
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

}
