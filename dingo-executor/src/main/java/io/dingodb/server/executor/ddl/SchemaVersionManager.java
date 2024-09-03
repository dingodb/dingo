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
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.meta.InfoSchemaService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
@Data
public class SchemaVersionManager {
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private AtomicLong lockOwner = new AtomicLong(0);

    public void unlockSchemaVersion(DdlJob job) {
        long jobId = job.getId();
        long ownId = lockOwner.get();
        if (ownId == jobId) {
            lockOwner.set(0);
            if (this.lock.writeLock().isHeldByCurrentThread()) {
                lock.writeLock().unlock();
                //LogUtils.info(log, "[ddl] lock schema ver unlock, jobId:{}", jobId);
                long sub = System.currentTimeMillis() - job.getLockVerTs();
                DingoMetrics.timer("lockSchemaVer").update(sub, TimeUnit.MILLISECONDS);
            }
        }
    }

    public Long setSchemaVersion(DdlJob job) {
        lockSchemaVersion(job.getId());
        job.setLockVerTs(System.currentTimeMillis());
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        return infoSchemaService.genSchemaVersion(1);
    }

    public void lockSchemaVersion(long jobId) {
        long ownId = this.lockOwner.get();
        if (ownId != jobId) {
            this.lock.writeLock().lock();
            this.lockOwner.set(jobId);
            LogUtils.debug(log, "[ddl] lock schema ver get lock, jobId:{}", jobId);
        }
    }
}
