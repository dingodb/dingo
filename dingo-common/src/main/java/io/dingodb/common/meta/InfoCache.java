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

package io.dingodb.common.meta;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InfoCache {
    public static final InfoCache infoCache = new InfoCache(16);
    ReentrantReadWriteLock lock;
    InfoSchema[] cache;
    long maxUpdatedSnapshotTS;

    private InfoCache(int capacity) {
        lock = new ReentrantReadWriteLock();
        this.cache = new InfoSchema[capacity];
    }

    public InfoSchema getLatest() {
        lock.readLock().lock();
        try {
            if (cache.length > 0) {
                return cache[0];
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void reset(int capacity) {
        lock.writeLock().lock();
        try {
            this.cache = new InfoSchema[capacity];
        } finally {
            lock.writeLock().unlock();
        }
    }

    public InfoSchema getByVersion(long version) {
        lock.readLock().lock();
        try {
            int length = cache.length;
            int ix = getIsIndex(version, length);
            if (ix < length && (ix != 0 || cache[ix].getSchemaMetaVersion() == version)) {
                return cache[ix];
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    private int getIsIndex(long version, int length) {
        int ix = 0;
        for (int i = 0; i < length; i ++) {
            InfoSchema infoSchema = cache[i];
            if (infoSchema.getSchemaMetaVersion() < version) {
                ix = i;
                break;
            }
        }
        return ix;
    }

    public boolean insert(InfoSchema is, long snapshotTs) {
        lock.writeLock().lock();
        try {
            long version = is.getSchemaMetaVersion();
            int ix = getIsIndex(version, cache.length);
            if (this.maxUpdatedSnapshotTS < snapshotTs) {
                this.maxUpdatedSnapshotTS = snapshotTs;
            }
            if (ix < cache.length && this.cache[ix].getSchemaMetaVersion() == version) {
                return true;
            }

            int len = getCacheCount();
            if (len < cache.length || ix < len) {
                cache[ix + 1] = cache[ix];
                cache[ix] = is;
                return true;
            }
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private int getCacheCount() {
        int i = 0;
        for (InfoSchema infoSchema : cache) {
            if (infoSchema != null) {
                i ++;
            }
        }
        return i;
    }
}
