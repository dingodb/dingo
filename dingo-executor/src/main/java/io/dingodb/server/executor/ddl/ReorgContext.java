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

package io.dingodb.server.executor.ddl;

import lombok.Data;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Data
public class ReorgContext {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    Map<Long, ReorgCtx> reorgCtxMap = new ConcurrentHashMap<>();

    public void rLock() {
        lock.readLock().lock();
    }

    public void rUnlock() {
        lock.readLock().unlock();
    }

    public void lock() {
        lock.writeLock().lock();
    }

    public void unLock() {
        if (lock.writeLock().isHeldByCurrentThread()) {
            lock.writeLock().unlock();
        }
    }

    public void putReorg(Long jobId, ReorgCtx reorgCtx) {
        reorgCtxMap.put(jobId, reorgCtx);
    }
}
