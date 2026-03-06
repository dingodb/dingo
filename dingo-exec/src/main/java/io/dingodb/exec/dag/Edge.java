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

package io.dingodb.exec.dag;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.exec.OperatorFactory;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Status;
import io.dingodb.exec.exception.TaskCancelException;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.memory.MemoryRevoker;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.AbstractParams;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

@Slf4j
@Setter
@Getter
@AllArgsConstructor
public class Edge {
    private static final ListenableFuture<?> NOT_BLOCKED = Futures.immediateFuture(null);
    private Vertex previous;
    private Vertex next;
    private CommonId partId;

    public Edge(Vertex previous, Vertex next) {
        this.previous = previous;
        this.next = next;
    }

    public boolean transformToNext(Object[] tuple) {
        return transformToNext(Context.builder().keyState(new ArrayList<>()).build(), tuple);
    }

    public boolean transformToNext(Context context, Object[] tuple) {
        if (next.getTask().getStatus() == Status.CANCEL) {
            LogUtils.info(log, "task status is cancel");
            throw new TaskCancelException("task is cancel");
        } else if (next.getTask().getStatus() == Status.STOPPED) {
            return false;
        }
        Operator operator = OperatorFactory.getInstance(next.getOp());
        next.getData().incCnt();
        boolean revoke = needRevoke(next.getData(), operator);
        if (revoke) {
            while (true) {
                ListenableFuture<?> blocked = handleMemoryRevoke(next.getData(), operator);
                if (blocked != null && !blocked.isDone()) {
                    try {
                        blocked.get();
                        checkExecutorFinishedRevoking((MemoryRevoker) operator, blocked, next.getData());
                        LogUtils.info(log, "memory revoke locked continue..");
                        break;
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    blocked = waitingForMemory(next.getData());
                    if (blocked.isDone()) {
                        break;
                    }
                }
            }
        }
        return operator.push(context.setPin(previous.getPin()), tuple, next);
    }

    public void fin(Fin fin) {
        OperatorFactory.getInstance(next.getOp()).fin(previous.getPin(), fin, next);
    }

    public boolean needRevoke(AbstractParams param, Operator operator) {
        if (!(operator instanceof MemoryRevoker)) {
            return false;
        }
        if (!ScopeVariables.enableSpill()) {
            return false;
        }
        MemoryRevoker memoryRevoker = (MemoryRevoker) operator;
        return memoryRevoker.getMemoryAllocatorCtx(param) != null;
    }

    public ListenableFuture<?> handleMemoryRevoke(AbstractParams param, Operator operator) {
        MemoryRevoker memoryRevoker = (MemoryRevoker) operator;
        boolean memoryRevokingRequested = memoryRevoker.getMemoryAllocatorCtx(param).isMemoryRevokingRequested();
        if (memoryRevokingRequested) {
            ListenableFuture<?> future = memoryRevoker.startMemoryRevoke(param);
            LogUtils.info(log, "start memory revoke future:{}, param:{}, pre pin:{}, param cnt:{}",
                future, param, previous.getPin(), param.getCnt());
            return future;
        }
        return null;
    }

    public ListenableFuture<?> waitingForMemory(AbstractParams param) {
        Operator operator = OperatorFactory.getInstance(next.getOp());
        MemoryRevoker memoryRevoker = (MemoryRevoker) operator;
        ListenableFuture<?> blocked = memoryRevoker.getMemoryAllocatorCtx(param).isWaitingForMemory();
        if (blocked != null && !blocked.isDone()) {
            LogUtils.info(log, "waiting for memory future:{}", blocked);
            return blocked;
        } else {
            return NOT_BLOCKED;
        }
    }

    public void checkExecutorFinishedRevoking(MemoryRevoker memoryRevoker, ListenableFuture<?> future,
                                              AbstractParams param) {
        if (future.isDone()) {
            checkException(future);
            memoryRevoker.finishMemoryRevoke(param);
            if (memoryRevoker.getMemoryAllocatorCtx(param) != null) {
                memoryRevoker.getMemoryAllocatorCtx(param).resetMemoryRevokingRequested();
            }
        }
    }

    public static void checkException(ListenableFuture<?> future) {
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
