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

package io.dingodb.common.memory;

import io.dingodb.common.mysql.DingoErrUtil;
import io.dingodb.common.mysql.scope.ScopeVariables;
import lombok.extern.slf4j.Slf4j;

import static io.dingodb.common.mysql.error.ErrorCode.ErrOutOfMemory;

@Slf4j
public class QueryMemoryPool extends BlockingMemoryPool {
    protected MemoryPool planMemPool;

    public QueryMemoryPool(String name, long limit, MemoryPool parent) {
        super(name, limit, parent, MemoryType.QUERY);
        this.planMemPool = this.getOrCreatePool("planner", limit, MemoryType.PLANER);
    }

    @Override
    protected boolean inheritParentFuture() {
        //不阻塞小查询
        return getMemoryUsage() > MemorySetting.DEFAULT_LESS_REVOKE_BYTES;
    }

    public MemoryPool getPlanMemPool() {
        return planMemPool;
    }

    @Override
    protected void outOfMemory(String memoryPool, long usage, long allocating, long limit, Boolean reserved) {
        log.warn("Current Query MemoryPool: " + this.printDetailInfo(0));
        throw DingoErrUtil.newStdErr(ErrOutOfMemory);
    }
}
