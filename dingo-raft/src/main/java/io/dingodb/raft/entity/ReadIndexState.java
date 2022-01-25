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

package io.dingodb.raft.entity;

import io.dingodb.raft.closure.ReadIndexClosure;
import io.dingodb.raft.util.Bytes;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class ReadIndexState {
    /** The committed log index*/
    private long index = -1;
    /** User request context*/
    private final Bytes requestContext;
    /** User ReadIndex closure*/
    private final ReadIndexClosure done;
    /** Request start timestamp*/
    private final long startTimeMs;

    public ReadIndexState(Bytes requestContext, ReadIndexClosure done, long startTimeMs) {
        super();
        this.requestContext = requestContext;
        this.done = done;
        this.startTimeMs = startTimeMs;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public Bytes getRequestContext() {
        return requestContext;
    }

    public ReadIndexClosure getDone() {
        return done;
    }

}
