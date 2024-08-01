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

import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.util.Pair;
import io.dingodb.meta.InfoSchemaService;
import lombok.Data;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Data
public class SchemaVersionManager {
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private AtomicLong lockOwner = new AtomicLong(0);

    public void unlockSchemaVersion(long jobId) {
        long ownId = lockOwner.get();
        if (ownId == jobId) {
            lockOwner.set(0);
            if (this.lock.writeLock().isHeldByCurrentThread()) {
                lock.writeLock().unlock();
            }
        }
    }

    public Long setSchemaVersion(DdlJob job) {
        lockSchemaVersion(job.getId());
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        return infoSchemaService.genSchemaVersion(1);
    }

    public void lockSchemaVersion(long jobId) {
        long ownId = this.lockOwner.get();
        if (ownId != jobId) {
            this.lock.writeLock().lock();
            this.lockOwner.set(jobId);
        }
    }
}
