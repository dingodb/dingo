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

import io.dingodb.common.mysql.scope.ScopeVariables;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemoryManager {
    protected static MemoryManager instance = new MemoryManager();

    @Getter
    protected GlobalMemoryPool globalMemoryPool;

    protected TpMemoryPool tpMemoryPool;

    protected ApMemoryPool apMemoryPool;

    protected MemoryPool cacheMemoryPool;

    public static MemoryManager getInstance() {
        return instance;
    }

    protected MemoryManager() {
        init();
    }

    public void init() {
        globalMemoryPool =
            new GlobalMemoryPool(MemoryType.GLOBAL.getExtensionName(), MemorySetting.UNLIMITED_SIZE);
        cacheMemoryPool = globalMemoryPool
            .getOrCreatePool(MemoryType.CACHE.getExtensionName(), MemorySetting.UNLIMITED_SIZE, MemoryType.CACHE);
        tpMemoryPool = (TpMemoryPool) globalMemoryPool
            .getOrCreatePool(MemoryType.GENERAL_TP.getExtensionName(), MemorySetting.UNLIMITED_SIZE,
                MemoryType.GENERAL_TP);
        apMemoryPool = (ApMemoryPool) globalMemoryPool
            .getOrCreatePool(MemoryType.GENERAL_AP.getExtensionName(), MemorySetting.UNLIMITED_SIZE,
                MemoryType.GENERAL_AP);
    }

    public void adjustMemoryLimit(long globalLimit) {
        //globalMemoryLimitRatio
        long newGlobalLimit = Math.round(globalLimit * 1.0);
        globalMemoryPool.setMaxLimit(newGlobalLimit);
        tpMemoryPool.setMaxLimit(newGlobalLimit);
        apMemoryPool.setMaxLimit(newGlobalLimit);
        cacheMemoryPool.setMaxLimit(newGlobalLimit);
        tpMemoryPool.setMaxLimit(Math.round(newGlobalLimit * MemorySetting.TP_HIGH_MEMORY_PROPORTION));
        apMemoryPool.setMaxLimit(Math.round(newGlobalLimit * MemorySetting.AP_HIGH_MEMORY_PROPORTION));
    }

    public MemoryPool createQueryMemoryPool(boolean ap, String traceId) {
        return createQueryMemoryPool(ap, traceId, ScopeVariables.perQueryMemoryLimit());
    }

    public MemoryPool createQueryMemoryPool(boolean ap, String traceId, long queryMemoryLimit) {
        queryMemoryLimit = checkMemoryLimit(queryMemoryLimit);

        if (ap) {
            return apMemoryPool.getOrCreatePool(traceId, queryMemoryLimit, MemoryType.QUERY);
        } else {
            return tpMemoryPool.getOrCreatePool(traceId, queryMemoryLimit, MemoryType.QUERY);
        }
    }

    private long checkMemoryLimit(long queryMemoryLimit) {
        if (queryMemoryLimit == MemorySetting.USE_DEFAULT_MEMORY_LIMIT_VALUE) {
            // use the default limit value set by drds
            // If not set by user, calculate a default value (1/4 of the general
            // pool size)
            long globalLimit = this.globalMemoryPool.getMaxLimit();
            // By default allow a single query to use up to 1/4 memory
            queryMemoryLimit = (long) (globalLimit * MemorySetting.DEFAULT_ONE_QUERY_MAX_MEMORY_PROPORTION);
        }
        return queryMemoryLimit;
    }
}
