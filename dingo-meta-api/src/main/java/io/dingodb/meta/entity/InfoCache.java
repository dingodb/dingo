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

package io.dingodb.meta.entity;

import io.dingodb.common.log.LogUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class InfoCache {
    public static final InfoCache infoCache = new InfoCache(16);
    ReentrantReadWriteLock lock;
    public InfoSchema[] cache;
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
            if (ix == -1) {
                return null;
            }
            if (ix < length && (ix != 0 || cache[ix].getSchemaMetaVersion() == version)) {
                return cache[ix];
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    private int getIsIndex(long version, int length) {
        int ix = -1;
        for (int i = 0; i < length; i ++) {
            InfoSchema infoSchema = cache[i];
            if (infoSchema == null) {
                continue;
            }
            if (infoSchema.getSchemaMetaVersion() <= version) {
                ix = i;
                break;
            }
        }
        return ix;
    }

    public boolean insert(InfoSchema is, long snapshotTs) {
        if (is == null) {
            return false;
        }
//        if (is.schemaMap.containsKey("DINGO")) {
//            int size = is.schemaMap.get("DINGO").getTables().size();
//            LogUtils.info(log, "is dingo table size:{}, schemaVer:{}", size, is.schemaMetaVersion);
//        }
        lock.writeLock().lock();
        long version = is.getSchemaMetaVersion();
        try {
            int ix = getIsIndex(version, cache.length);
            //LogUtils.info(log, "is insert before schemaVer:{}, get index:{}, cache size:{}", version, ix, getCacheCount());
            if (ix == -1) {
                ix = 0;
            }
            if (this.maxUpdatedSnapshotTS < snapshotTs) {
                this.maxUpdatedSnapshotTS = snapshotTs;
            }
            if (ix < cache.length && this.cache[ix] != null) {
                if (this.cache[ix].getSchemaMetaVersion() == version) {
                    return true;
                }
            }

            int len = getCacheCount();
            if (ix < len || len < cache.length) {
                // has free space, grown the slice
                for (int i = cache.length - 1; i > ix; i --) {
                    cache[i] = cache[i - 1];
                }
                cache[ix] = is;
                return true;
            }
            return false;
        } finally {
            lock.writeLock().unlock();
            //LogUtils.info(log, "is insert after schemaVer:{}, cache size:{}", version, getCacheCount());
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
