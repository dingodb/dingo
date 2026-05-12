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

package io.dingodb.exec.memory;

import io.dingodb.common.error.MemoryNotEnoughException;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.memory.MemoryPool;
import io.dingodb.common.util.Utils;
import io.dingodb.tool.api.MemoryAllocatorCtx;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemoryController {
    private final MemoryPool memoryPool;
    private final MemoryAllocatorCtx memoryAllocatorCtx;
    private final long notifyMemorySize;
    private volatile boolean closed = false;

    private final Object lock = new Object();

    public MemoryController(MemoryPool pool) {
        memoryPool = pool;
        memoryAllocatorCtx = new DefaultMemoryAllocatorCtx(memoryPool);
        notifyMemorySize = memoryPool.getMaxLimit() / 2;
    }

    public MemoryController(MemoryPool pool, MemoryAllocatorCtx ctx) {
        memoryPool = pool;
        memoryAllocatorCtx = ctx;
        notifyMemorySize = memoryPool.getMaxLimit() / 2;
    }

    public void allocate(long memorySize) {
        synchronized (lock) {
            while (!closed) {
                try {
                    LogUtils.info(log, "memory controller, size:{}, before reserved memory:{}",
                        memorySize, memoryAllocatorCtx.getReservedAllocated());
                    memoryAllocatorCtx.allocateReservedMemory(memorySize);
                    LogUtils.info(log, "memory controller, after reserved memory:{}",
                        memoryAllocatorCtx.getReservedAllocated());
                    break;
                } catch (MemoryNotEnoughException e) {
                    if (memorySize > memoryPool.getMaxLimit()) {
                        throw e;
                    }
                    //try {
                    //    lock.wait();
                    //} catch (InterruptedException ie) {
                    //    throw new RuntimeException(ie);
                    //}
                    LogUtils.info(log, "memory controller blocked");
                    Utils.sleep(1000);
                }
            }
        }
    }

    public void release(long memorySize) {
        synchronized (lock) {
            memoryAllocatorCtx.releaseReservedMemory(memorySize, false);
            //if (memoryAllocatorCtx.getAllAllocated() <= notifyMemorySize) {
            //    lock.notifyAll();
            //}
        }
    }

    public void clear() {
        synchronized (lock) {
            memoryAllocatorCtx.releaseReservedMemory(memoryAllocatorCtx.getReservedAllocated(), true);
        }
    }

    public void close() {
        synchronized (lock) {
            closed = true;
            //lock.notifyAll();
            this.memoryAllocatorCtx.close();
        }
    }
}
