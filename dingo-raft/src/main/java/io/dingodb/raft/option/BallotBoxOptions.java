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

package io.dingodb.raft.option;

import io.dingodb.raft.FSMCaller;
import io.dingodb.raft.closure.ClosureQueue;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class BallotBoxOptions {
    private FSMCaller waiter;
    private ClosureQueue closureQueue;

    public FSMCaller getWaiter() {
        return this.waiter;
    }

    public void setWaiter(FSMCaller waiter) {
        this.waiter = waiter;
    }

    public ClosureQueue getClosureQueue() {
        return this.closureQueue;
    }

    public void setClosureQueue(ClosureQueue closureQueue) {
        this.closureQueue = closureQueue;
    }
}
