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

package io.dingodb.common.ddl;

import lombok.Data;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Data
public class WaitSchemaSyncedController {
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Map<Long, Long> jobMap = new ConcurrentHashMap<>();
    private AtomicBoolean once = new AtomicBoolean(true);

    public void setOnceVal(Boolean onceVal) {
        this.once.set(onceVal);
    }

    public boolean isSynced(long id) {
        lock.readLock().lock();
        try {
            return !jobMap.containsKey(id);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void synced(DdlJob job) {
        lock.writeLock().lock();
        jobMap.remove(job.getId());
        lock.writeLock().unlock();
    }
}
