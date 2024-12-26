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
    //   private Function<String, Object> onEvict;
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
        // 按照访问顺序排序的LinkedHashMap，设置初始容量和加载因子
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
//            lruPlanCache.onEvict = null;  // 初始化时暂设为null，可按实际需求在使用时赋值
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

    // put the plan cache k-v into LRU container
    public void put(PlanCacheKey key, PlanCacheValue value) {
        readWriteLock.writeLock().lock();
        try {
            cacheMap.put(key, value);
            accessOrderMap.put(key, value);
            size++;
            if (size > capacity) {
                removeOldest();
            }
//            memoryControl();
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

    // Remove the oldest plan cache
//    private void removeOldest() {
//        if (!lruList.Empty()) {
//            Element element = lruList.Front();
//            lruList.Remove(element);
//            Map<Element, Object> bucket = buckets.get(((PlanCacheEntry) element.Value).PlanKey);
//            if (bucket != null) {
//                bucket.remove(element);
//            }
//            size--;
//        }
//    }

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

    // return currnt cache size
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

    // return the memory usage
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

    // 对应Close方法，在关闭会话时对LRUPlanCache做清理工作
    public void close() {
        deleteAll();
    }

    // 对应removeOldest方法，移除缓存中最旧的元素
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

//    public static long getObjectSize(Object o) {
//        return unsafe.getObjectSize(o);
//    }
}

//    private void removeFromBucket(Element lru) {
//        Map<Element, Object> bucket = buckets.get(((PlanCacheEntry) lru.Value).PlanKey);
//        if (bucket != null) {
//            bucket.remove(lru);
//        }
//    }

//    public void removeFromBucket(Element element) {
//        PlanCacheKey hash = ((PlanCacheEntry) element).planKey;
//        Map<Element, Object> bucket = buckets.get(hash);
//        if (bucket != null) {
//            bucket.remove(element);
//            if (bucket.isEmpty()) {
//                buckets.remove(hash);
//            }
//        }
//    }

//    // 对应memoryControl方法，通过配额和保护值控制内存
//    public void memoryControl() {
//        if (quota == 0 || guard == 0) {
//            return;
//        }
//
//        long memUsed = Memory.instanceMemUsed();
//        while (memUsed > (long) (quota * (1.0 - guard)) && size > 0) {
//            removeOldest();
//            memUsed = Memory.instanceMemUsed();
//        }
//    }
//
//    // 对应PickPlanFromBucket方法，从桶中挑选一个计划（这里按原代码逻辑，依据类型兼容性检查来挑选）
//    public static Element pickFromBucket(Map<Element, Object> bucket, Object paramTypes) {
//        for (Element k : bucket.keySet()) {
//            if (TypeChecker.checkTypesCompatibility4PC(((PlanCacheValue) ((PlanCacheEntry) k).getPlanValue()).ParamTypes, paramTypes)) {
//                return k;
//            }
//        }
//        return null;
//    }
//
//    public static void updateInstancePlanNum(PlanCacheEntry in, PlanCacheEntry out) {
//        if (in != null && out != null) { // 替换计划的情况，原代码中直接返回，这里保持一致逻辑
//            return;
//        }
//    }
//    public void moveFront(PlanCacheEntry in,InstancePlanCache planCache){
//
//    }
