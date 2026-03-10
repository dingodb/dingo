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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.ExecutionContext;
import io.dingodb.common.memory.MemoryPool;
import io.dingodb.common.memory.MemoryPoolUtils;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.exec.memory.OperatorMemoryAllocatorCtx;
import io.dingodb.tool.api.WindowService;
import lombok.Getter;
import lombok.Setter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@JsonTypeName("window")
@JsonPropertyOrder({"funName"})
public class WindowFunctionParam extends AbstractParams implements RevokerParams {

    @JsonProperty("funName")
    String funName;

    @Getter
    List<Object[]> list = new ArrayList<>();

    @Setter
    @Getter
    WindowService windowService;

    @Getter
    private ExecutionContext executionContext;

    private OperatorMemoryAllocatorCtx memoryAllocatorCtx;

    @Getter
    AtomicLong size;

    protected long spillCnt = 0;


    public WindowFunctionParam(WindowService windowService, ExecutionContext executionContext) {
        this.windowService = windowService;
        this.executionContext = executionContext;
        this.size = new AtomicLong(0);
        if (!executionContext.isInnerSql()) {
            String name = "windowFun" + UUID.randomUUID();
            MemoryPool memoryPool =
                MemoryPoolUtils.createOperatorTmpTablePool(name, executionContext.getMemoryPool());
            this.memoryAllocatorCtx = new OperatorMemoryAllocatorCtx(memoryPool, ScopeVariables.enableSpill());
        }
    }

    @Override
    public MemoryPool getQueryMemoryPool() {
        if (this.memoryAllocatorCtx != null && this.executionContext != null) {
            return this.executionContext.getMemoryPool();
        }
        return null;
    }

    @Override
    public OperatorMemoryAllocatorCtx getMemoryAllocatorCtx() {
        return memoryAllocatorCtx;
    }

    @Override
    public void addSpillCnt(int spillCnt) {
        this.spillCnt += spillCnt;
    }

    @Override
    public long getCacheSize() {
        return list.size();
    }

    public void clear() {
        list.clear();
        if (this.memoryAllocatorCtx != null) {
            this.memoryAllocatorCtx.close();
        }
    }
}
