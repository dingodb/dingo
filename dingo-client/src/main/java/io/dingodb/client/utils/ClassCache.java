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

package io.dingodb.client.utils;


import io.dingodb.client.IBaseDingoMapper;
import io.dingodb.client.configuration.ClassConfig;
import io.dingodb.client.configuration.Configuration;
import io.dingodb.sdk.common.DingoClientException;

import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;

public class ClassCache {
    private static final ClassCache instance = new ClassCache();

    public static ClassCache getInstance() {
        return instance;
    }

    enum PolicyType {
        READ,
        WRITE,
        BATCH,
        SCAN,
        QUERY
    }

    private final Map<Class<?>, ClassCacheEntry<?>> cacheMap = new HashMap<>();
    private final Map<String, ClassConfig> classesConfig = new HashMap<>();
    private final Map<String, ClassCacheEntry<?>> storedNameToCacheEntry = new HashMap<>();

    private final Object lock = new Object();

    private ClassCache() {
    }

    @SuppressWarnings("unchecked")
    public <T> ClassCacheEntry<T> loadClass(@NotNull Class<T> clazz, IBaseDingoMapper mapper) {
        if (clazz.isPrimitive() || clazz.equals(Object.class) || clazz.equals(String.class)
                || clazz.equals(Character.class) || Number.class.isAssignableFrom(clazz)) {
            return null;
        }

        ClassCacheEntry<T> entry = (ClassCacheEntry<T>) cacheMap.get(clazz);
        if (entry == null || entry.isNotConstructed()) {
            synchronized (lock) {
                entry = (ClassCacheEntry<T>) cacheMap.get(clazz);
                if (entry == null) {
                    try {
                        // Construct a class cache entry. This must be done in 2 steps, one creating the entry
                        // and the other finalizing construction of it.
                        // This is to cater for classes  which recursively refer to themselves, such as
                        // public static class A {
                        //      @DingoKey
                        //      public int id;
                        //      public A a;
                        //  }
                        entry = new ClassCacheEntry<>(clazz, mapper, getClassConfig(clazz));
                    } catch (NotAnnotatedClass nae) {
                        return null;
                    }
                    cacheMap.put(clazz, entry);
                    try {
                        entry.construct();
                    } catch (IllegalArgumentException iae) {
                        cacheMap.remove(clazz);
                        return null;
                    } catch (Exception e) {
                        cacheMap.remove(clazz);
                        throw e;
                    }
                }
            }
        }
        return entry;
    }

    // package visibility
    void setStoredName(@NotNull ClassCacheEntry<?> entry, @NotNull String name) {
        ClassCacheEntry<?> existingEntry = storedNameToCacheEntry.get(name);
        if (existingEntry != null && !(existingEntry.equals(entry))) {
            String errorMessage = String.format("Stored name of \"%s\" is used for both %s and %s",
                    name, existingEntry.getUnderlyingClass().getName(), entry.getUnderlyingClass().getName());
            throw new DingoClientException(errorMessage);
        } else {
            storedNameToCacheEntry.put(name, entry);
        }
    }

    public ClassCacheEntry<?> getCacheEntryFromStoredName(@NotNull String name) {
        return storedNameToCacheEntry.get(name);
    }


    public boolean hasClass(Class<?> clazz) {
        return cacheMap.containsKey(clazz);
    }

    /**
     * This method is typically only used for testing.
     */
    public void clear() {
        this.cacheMap.clear();
        this.classesConfig.clear();
        TypeUtils.clear();
        this.storedNameToCacheEntry.clear();
    }

    public void addConfiguration(@NotNull Configuration configuration) {
        for (ClassConfig thisConfig : configuration.getClasses()) {
            classesConfig.put(thisConfig.getClassName(), thisConfig);
        }
    }

    public ClassConfig getClassConfig(String className) {
        return classesConfig.get(className);
    }

    public ClassConfig getClassConfig(Class<?> clazz) {
        return classesConfig.get(clazz.getName());
    }

    public boolean hasClassConfig(String className) {
        return classesConfig.containsKey(className);
    }

    public boolean hasClassConfig(Class<?> clazz) {
        return classesConfig.containsKey(clazz.getName());
    }
}
