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
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.TaskStatus;
import io.dingodb.exec.memory.MemoryRevoker;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.AbstractParams;
import io.dingodb.exec.operator.params.HashJoinParam;
import io.dingodb.exec.operator.params.SortParam;
import io.dingodb.exec.operator.params.WindowFunctionParam;
import io.dingodb.tool.api.MemoryAllocatorCtx;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.List;

@Slf4j
public class WindowFunctionOperator extends SoleOutOperator implements MemoryRevoker {
    public static final WindowFunctionOperator INSTANCE = new WindowFunctionOperator();

    private WindowFunctionOperator() {

    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        WindowFunctionParam param = vertex.getParam();
        synchronized (param) {
            if (param.getSize() != null && param.getSize().get() > ScopeVariables.joinSpillSize()
                && !param.getExecutionContext().isInnerSql()) {
                param.getMemoryAllocatorCtx().allocateRevocableMemory(param.getSize().get());
                param.getSize().set(0);
            }
            long size = ObjectSizeUtils.calculateSize(tuple);
            param.getSize().addAndGet(size);
            param.getList().add(tuple);
        }
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        // push next
        WindowFunctionParam param = vertex.getParam();

        try {
            Iterator response = param.getWindowService().transform(param.getList().iterator());
            while (response.hasNext()) {
                Object[] tuple1 = (Object[]) response.next();
                vertex.getSoleEdge().transformToNext(tuple1);
            }
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            TaskStatus taskStatus = new TaskStatus();
            taskStatus.setStatus(false);
            taskStatus.setTaskId(vertex.getTask().getId().toString());
            taskStatus.setErrorMsg(e.getMessage());
            vertex.getSoleEdge().fin(FinWithException.of(taskStatus));
            return;
        }
        // push fin
        vertex.getSoleEdge().fin(fin);
        param.clear();
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke(AbstractParams param) {
        synchronized (param) {
            WindowFunctionParam windowFunctionParam = (WindowFunctionParam) param;
            if (windowFunctionParam.isSpilling()) {
                while (windowFunctionParam.getSpillFuture() == null) {
                    Utils.sleep(50);
                }
                return windowFunctionParam.getSpillFuture();
            }
            long revocable = windowFunctionParam.getMemoryAllocatorCtx().getRevocableAllocated();
            if (revocable > 1024 * 1024 * 4) {
                windowFunctionParam.addSpillCnt(1);
                return spillToDisk(windowFunctionParam);
            } else {
                return null;
            }
        }
    }

    @Override
    public void finishMemoryRevoke(AbstractParams param) {
        // finish -> releaseMemory
        WindowFunctionParam windowFunParam = (WindowFunctionParam) param;
        MemoryAllocatorCtx memoryAllocatorCtx = windowFunParam.getMemoryAllocatorCtx();
        memoryAllocatorCtx.releaseRevocableMemory(memoryAllocatorCtx.getRevocableAllocated(), true);
        LogUtils.info(log, "window function finish memory revoke, release revocable memory");
    }

    @Override
    public MemoryAllocatorCtx getMemoryAllocatorCtx(AbstractParams param) {
        WindowFunctionParam windowFunctionParam = (WindowFunctionParam) param;
        return windowFunctionParam.getMemoryAllocatorCtx();
    }

    public ListenableFuture<?> spillToDisk(WindowFunctionParam windowFunctionParam) {
        LogUtils.info(log, "start spill to disk");

        windowFunctionParam.setSpilling(true);
        SettableFuture<?> future = SettableFuture.create();
        new Thread(() -> {
            Utils.sleep(10000);

            releaseSpill(windowFunctionParam);
            future.set(null);
        }).start();
        return future;
    }
}
