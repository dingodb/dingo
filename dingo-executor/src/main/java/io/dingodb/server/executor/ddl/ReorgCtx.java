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

package io.dingodb.server.executor.ddl;

import lombok.Builder;
import lombok.Data;

import java.util.concurrent.CompletableFuture;

@Data
public class ReorgCtx {
    private CompletableFuture<String> done;
    //private boolean done;
    private long rowCount;
    private long notifyCancelReorgJob;
    private Object doneKey;

    @Builder
    public ReorgCtx(CompletableFuture<String> done, long rowCount, long notifyCancelReorgJob, Object doneKey) {
        this.done = done;
        this.rowCount = rowCount;
        this.notifyCancelReorgJob = notifyCancelReorgJob;
        this.doneKey = doneKey;
    }

    public ReorgCtx() {
    }

    public boolean isReorgCanceled() {
        return notifyCancelReorgJob == 1;
    }

    public void incrementCount(long count) {
        rowCount += count;
    }
}
