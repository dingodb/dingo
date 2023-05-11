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

import io.dingodb.client.common.Key;

import java.util.HashMap;
import java.util.Map;

public class LoadedObjectResolver {

    private static class LoadedObjectMap {
        private int referenceCount = 0;
        private final Map<Key, Object> objectMap = new HashMap<>();
    }

    private static final ThreadLocal<LoadedObjectMap> threadLocalObjects
        = ThreadLocal.withInitial(LoadedObjectMap::new);

    public static void begin() {
        LoadedObjectMap map = threadLocalObjects.get();
        map.referenceCount++;
    }

    public static void end() {
        LoadedObjectMap map = threadLocalObjects.get();
        map.referenceCount--;
        if (map.referenceCount == 0) {
            map.objectMap.clear();
        }
    }

    public static void setObjectForCurrentKey(Object object) {
        Key currentKey = ThreadLocalKeySaver.get();
        LoadedObjectMap map = threadLocalObjects.get();
        if (currentKey != null) {
            map.objectMap.put(currentKey, object);
        }
    }

    public static Object get(Key key) {
        LoadedObjectMap map = threadLocalObjects.get();
        return map.objectMap.get(key);
    }
}
