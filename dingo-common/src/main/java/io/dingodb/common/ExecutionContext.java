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

package io.dingodb.common;

import io.dingodb.common.memory.MemoryPool;
import io.dingodb.common.memory.QueryMemoryPoolHolder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;

import java.util.Properties;

@EqualsAndHashCode(callSuper = true)
@Data
public class ExecutionContext extends ExecuteVariables{
    public ExecutionContext() {

    }

    public ExecutionContext(Properties properties) {
        this.iterationLimit = getIterationLimit(properties);
        this.isJoinConcurrency = isJoinConcurrency(properties);
        this.concurrencyLevel = getConcurrencyLevel(properties);
        this.isInsertCheckInplace = isInsertCheckInplace(properties);
        this.isExecutorShuffle = isExecutorShuffle(properties);
        this.queryId = properties.getProperty("queryId", null);
    }

    private long maxTimeout;
    private boolean isSelect;
    @Setter
    private String queryId;
    @Setter
    private String user;
    @Setter
    private String host;

    private String traceId;

    private boolean innerSql;

    private QueryMemoryPoolHolder memoryPoolHolder = new QueryMemoryPoolHolder();

    public MemoryPool getMemoryPool() {
        return memoryPoolHolder.getQueryMemoryPool();
    }

    public void setMemoryPool(MemoryPool memoryPool) {
        memoryPoolHolder.initQueryMemoryPool(memoryPool);
    }

    public void renewMemoryPoolHolder() {
        memoryPoolHolder.destroy();
        this.memoryPoolHolder = new QueryMemoryPoolHolder();
    }

    public void clearAllMemoryPool() {
        memoryPoolHolder.destroy();
    }
}
