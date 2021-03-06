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

import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.raft.Node;
import io.dingodb.raft.Status;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

@Slf4j
public class ReadIndexRunner {

    private final Executor executor;
    private final Node node;
    private final Function<RaftRawKVOperation, Object> executeFunc;

    public ReadIndexRunner(Node node, Function<RaftRawKVOperation, Object> executeFunc) {
        this.node = node;
        this.executeFunc = executeFunc;
        this.executor = Executors.executor(node.getGroupId() + "-read-index");
    }

    public <T> CompletableFuture<T> readIndex(RaftRawKVOperation operation) {
        CompletableFuture<T> future = new CompletableFuture<>();
        this.node.readIndex(ByteArrayUtils.EMPTY_BYTES, new ReadIndexClosure<T>(future, operation));
        return future;
    }

    @AllArgsConstructor
    private class ReadIndexClosure<T> extends io.dingodb.raft.closure.ReadIndexClosure {

        private final CompletableFuture<T> future;
        private final RaftRawKVOperation operation;

        @Override
        public void run(Status status, long index, byte[] reqCtx) {
            if (status.isOk()) {
                Executors.execute("read-index-exec", () -> future.complete((T) executeFunc.apply(operation)));
                return;
            }
            executor.execute(() -> {
                if (node.isLeader()) {
                    log.warn("Fail to [get] with 'ReadIndex': {}, try to applying to the state machine.", status);
                    // If 'read index' read fails, try to applying to the state machine at the leader node
                    RaftRawKVOperation.sync().applyOnNode(node).whenCompleteAsync((r, e) -> {
                        if (e == null) {
                            future.complete((T) executeFunc.apply(operation));
                        } else {
                            future.completeExceptionally(e);
                        }
                    }, Executors.executor("read-index-exec"));
                } else {
                    log.warn("Fail to [get] with 'ReadIndex': {}.", status);
                    // Client will retry to leader node
                    future.completeExceptionally(new RuntimeException(
                        String.format(
                            "Read index error, code: %d, msg: %s, raft: %s.",
                            status.getCode(),
                            status.getErrorMsg(),
                            status.getRaftError()
                        )));
                }
            });
        }
    }
}
