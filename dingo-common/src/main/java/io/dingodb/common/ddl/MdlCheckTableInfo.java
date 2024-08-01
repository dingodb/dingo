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
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Data
@Slf4j
public class MdlCheckTableInfo {
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private long newestVer;
    Map<Long, Long> jobsVerMap = new ConcurrentHashMap<>();
    Map<Long, String> jobsIdsMap = new ConcurrentHashMap<>();

    public void wLock() {
        lock.writeLock().lock();
    }

    public void wUnlock() {
        lock.writeLock().unlock();
    }

    public void rLock() {
        lock.readLock().lock();
    }

    public void rUnlock() {
        lock.readLock().unlock();
    }

}
