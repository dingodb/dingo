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

package io.dingodb.exec.operator;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.memory.ObjectSizeUtils;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.TaskStatus;
import io.dingodb.exec.memory.MemoryRevoker;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.AbstractParams;
import io.dingodb.exec.operator.params.AggregateParams;
import io.dingodb.tool.api.MemoryAllocatorCtx;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;

@Slf4j
public final class AggregateOperator extends SoleOutOperator implements MemoryRevoker {
    public static final AggregateOperator INSTANCE = new AggregateOperator();

    private AggregateOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        AggregateParams params = vertex.getParam();
        // Track memory usage for the revocation scheduler
        if (params.getMemoryAllocatorCtx() != null) {
            long tupleSize = ObjectSizeUtils.calculateSize(tuple);
            params.getMemoryAllocatorCtx().allocateRevocableMemory(tupleSize);
        }
        params.addTuple(tuple);
        return true;
    }

    @Override
    public  void fin(int pin, Fin fin, Vertex vertex) {
        AggregateParams params = vertex.getParam();
        Edge edge = vertex.getSoleEdge();
        try {
            params.prepareResults();
            for (Object[] t : params.getCache()) {
                if (!edge.transformToNext(t)) {
                    break;
                }
            }
        } catch (Exception e) {
            LogUtils.error(log, "[task-fin] fin exception:{}", e.getMessage(), e);
            TaskStatus taskStatus = new TaskStatus();
            taskStatus.setStatus(false);
            taskStatus.setTaskId(vertex.getTask().getId().toString());
            taskStatus.setErrorMsg(e.getMessage());
            edge.fin(FinWithException.of(taskStatus));
            return;
        }
        edge.fin(fin);
        // Reset
        params.clear();
    }

    // -------------------------------------------------------------------------
    // MemoryRevoker interface implementation
    // -------------------------------------------------------------------------

    @Override
    public ListenableFuture<?> startMemoryRevoke(AbstractParams param) {
        AggregateParams aggParams = (AggregateParams) param;
        SettableFuture<?> future = SettableFuture.create();
        new Thread(() -> {
            try {
                aggParams.spillCurrentBuffer();
                LogUtils.info(log, "AggregateOperator spilled input buffer during memory revocation");
                future.set(null);
            } catch (IOException e) {
                LogUtils.warn(log, "AggregateOperator failed to spill during memory revocation: {}",
                    e.getMessage());
                future.setException(e);
            }
        }, "aggregate-spill-thread").start();
        return future;
    }

    @Override
    public void finishMemoryRevoke(AbstractParams param) {
        AggregateParams aggParams = (AggregateParams) param;
        if (aggParams.getMemoryAllocatorCtx() != null) {
            aggParams.getMemoryAllocatorCtx().releaseRevocableMemory(
                aggParams.getMemoryAllocatorCtx().getRevocableAllocated(), true);
            aggParams.getMemoryAllocatorCtx().resetMemoryRevokingRequested();
            LogUtils.info(log, "AggregateOperator finished memory revoke, released revocable memory");
        }
    }

    @Override
    public MemoryAllocatorCtx getMemoryAllocatorCtx(AbstractParams param) {
        AggregateParams aggParams = (AggregateParams) param;
        return aggParams.getMemoryAllocatorCtx();
    }
}
