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

import com.google.common.base.Preconditions;

public class QueryMemoryPoolHolder {
    private volatile boolean closed = false;
    private MemoryPool queryMemoryPool = null;

    public synchronized void initQueryMemoryPool(MemoryPool queryMemoryPool) {
        try {
            checkNotClose();
            Preconditions.checkNotNull(queryMemoryPool, "Query memory pool can't be null!");
            if (this.queryMemoryPool != null && this.queryMemoryPool != queryMemoryPool) {
                throw new IllegalStateException(
                    "Trying to re-Init query memory pool, old: "
                        + this.queryMemoryPool.getFullName()
                        + ", new: " + queryMemoryPool.getFullName());
            }
        } catch (IllegalStateException t) {
            queryMemoryPool.destroy();
            throw t;
        }
        this.queryMemoryPool = queryMemoryPool;
    }

    public MemoryPool getQueryMemoryPool() {
        return queryMemoryPool;
    }

    public synchronized void destroy() {
        if (closed) {
            return;
        }

        if (queryMemoryPool != null) {
            queryMemoryPool.destroy();
            queryMemoryPool = null;
        }
        closed = true;
    }

    private void checkNotClose() {
        Preconditions.checkState(!closed, "Memory pool holder already closed!");
    }
}
