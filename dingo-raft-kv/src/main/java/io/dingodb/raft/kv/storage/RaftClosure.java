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

import io.dingodb.raft.Closure;
import io.dingodb.raft.Status;
import lombok.AllArgsConstructor;

import java.util.concurrent.CompletableFuture;

@AllArgsConstructor
public class RaftClosure<T> implements Closure {

    private final CompletableFuture<T> future;

    @Override
    public void run(Status status) {
        if (!status.isOk()) {
            future.completeExceptionally(new RuntimeException(
                String.format("Apply operation error, code: %d, msg: %s, raft: %s.", status.getCode(),
                    status.getErrorMsg(), status.getRaftError()
                )));
        }
    }

    public void complete(Object result) {
        future.complete((T) result);
    }
}
