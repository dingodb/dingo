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

import lombok.extern.slf4j.Slf4j;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

@Slf4j
public class MemoryPoolUtils {

    public static MemoryPool createNewPool(String name, long limit, MemoryType memoryType, MemoryPool parent) {
        MemoryPool mp;
        if (memoryType == MemoryType.QUERY) {
            mp = new QueryMemoryPool(name, limit, parent);
        } else if (memoryType == MemoryType.OPERATOR) {
            mp = new MemoryPool(name, limit, parent, MemoryType.OPERATOR);
        } else if (memoryType == MemoryType.SUBQUERY) {
            mp = new QueryMemoryPool(name, limit, parent);
        } else if (memoryType == MemoryType.GENERAL_TP) {
            mp = new TpMemoryPool(name, limit, limit, parent);
        } else if (memoryType == MemoryType.GENERAL_AP) {
            mp = new ApMemoryPool(name, limit, limit, parent);
        } else {
            mp = new MemoryPool(name, limit, parent, memoryType);
        }
        return mp;
    }

    public static MemoryPool createOperatorTmpTablePool(String memoryPoolName, MemoryPool rootPool) {
        return rootPool.getOrCreatePool(memoryPoolName, MemoryType.OPERATOR);
    }

    public static MemoryPool createCacheTmpTablePool(String memoryPoolName, MemoryPool rootPool) {
        return rootPool.getOrCreatePool(memoryPoolName, MemoryType.PROTOCOL_CACHE);
    }

}
