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
import io.dingodb.raft.Status;

import java.util.concurrent.ScheduledFuture;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public abstract class CatchUpClosure implements Closure {
    private long maxMargin;
    private ScheduledFuture<?> timer;
    private boolean hasTimer;
    private boolean errorWasSet;

    private final Status status = Status.OK();

    public Status getStatus() {
        return this.status;
    }

    public long getMaxMargin() {
        return this.maxMargin;
    }

    public void setMaxMargin(long maxMargin) {
        this.maxMargin = maxMargin;
    }

    public ScheduledFuture<?> getTimer() {
        return this.timer;
    }

    public void setTimer(ScheduledFuture<?> timer) {
        this.timer = timer;
        this.hasTimer = true;
    }

    public boolean hasTimer() {
        return this.hasTimer;
    }

    public boolean isErrorWasSet() {
        return this.errorWasSet;
    }

    public void setErrorWasSet(boolean errorWasSet) {
        this.errorWasSet = errorWasSet;
    }
}
