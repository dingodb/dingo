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

package io.dingodb.driver.plancache;

import io.dingodb.calcite.DingoParserContext;
import io.dingodb.common.log.LogUtils;
import lombok.extern.slf4j.Slf4j;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class LRUPlanCache {

    private int capacity;
    private int size;
    private ReadWriteLock readWriteLock;
    private long quota;
    private double guard;
    private long memoryUsageTotal;
    DingoParserContext context;
    private ConcurrentHashMap<PlanCacheKey, PlanCacheValue> cacheMap;
    private LinkedHashMap<PlanCacheKey, PlanCacheValue> accessOrderMap;


    public static LRUPlanCache newLRUPlanCache(int capacity, double guard, long quota,
                                               DingoParserContext context, boolean unusedParam) {
        LRUPlanCache lruPlanCache = new LRUPlanCache();

        lruPlanCache.cacheMap = new ConcurrentHashMap<>(capacity);
        long finalCapacity = capacity;
        lruPlanCache.accessOrderMap = new LinkedHashMap<PlanCacheKey, PlanCacheValue>(capacity) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<PlanCacheKey, PlanCacheValue> eldest) {
                return size() > finalCapacity;
            }
        };
        if (capacity < 1) {
            capacity = 1000;
            LogUtils.info(log, "capacity of LRU cache is less than 1, will use default value(1000) init cache");
        }

        lruPlanCache.capacity = capacity;
        lruPlanCache.size = 0;
        lruPlanCache.cacheMap = new ConcurrentHashMap<>();
        lruPlanCache.accessOrderMap = new LinkedHashMap<>();
        lruPlanCache.quota = quota;
        lruPlanCache.guard = guard;
        lruPlanCache.context = context;
        lruPlanCache.readWriteLock = new ReentrantReadWriteLock();
        return lruPlanCache;
    }

    public LRUPlanCache() {
    }

    public PlanCacheValue get(PlanCacheKey key) {
        readWriteLock.readLock().lock();
        try {
            PlanCacheValue value = cacheMap.get(key);
            if (value != null) {
                accessOrderMap.put(key, value);
            }
            return value;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public void put(PlanCacheKey key, PlanCacheValue value) {
        readWriteLock.writeLock().lock();
        try {
            cacheMap.put(key, value);
            accessOrderMap.put(key, value);
            size++;
            if (size > capacity) {
                removeOldest();
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    // delete multiple plan cache k-v map
    public void delete(PlanCacheKey key) {
        readWriteLock.writeLock().lock();
        try {
            PlanCacheValue value = cacheMap.get(key);
            if (value != null) {
                cacheMap.remove(key, value);
                accessOrderMap.remove(key, value);
                size--;
            }

        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    // Remove all plan cache for this session
    public void deleteAll() {
        if (this == null) {
            return;
        }
        readWriteLock.writeLock().lock();
        try {
            // reset all fields
            size = 0;
            cacheMap = new ConcurrentHashMap<>();
            accessOrderMap = new LinkedHashMap<>();
            memoryUsageTotal = 0;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public int size() {
        readWriteLock.readLock().lock();
        try {
            return size;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public Exception setCapacity(int capacity) {
        readWriteLock.writeLock().lock();
        try {
            if (capacity < 1) {
                LogUtils.error(log, "capacity of LRU cache should be at least 1");
            }
            this.capacity = capacity;
            while (size > capacity) {
                removeOldest();
            }
            return null;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public long memoryUsage() {
        if (this == null) {
            return 0;
        }
        readWriteLock.readLock().lock();
        try {
            return memoryUsageTotal;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public void close() {
        deleteAll();
    }

    private void removeOldest() {

        if (size > 0) {
            Map.Entry<PlanCacheKey, PlanCacheValue> lastEntry =
                accessOrderMap.entrySet().toArray(new Map.Entry[0])[size - 1];
            PlanCacheKey lastKey = lastEntry.getKey();
            PlanCacheValue value = lastEntry.getValue();
            if (lastKey == null) {
                return;
            }
            if (lastKey != null) {
                cacheMap.remove(lastEntry);
                accessOrderMap.remove(lastEntry);
                size--;
            }
        }
    }

    private static final Unsafe unsafe = getUnsafe();

    private static Unsafe getUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
