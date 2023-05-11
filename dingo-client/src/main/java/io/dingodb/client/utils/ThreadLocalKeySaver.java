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

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Save the keys.
 * Note that this is effectively a stack of keys, as A can load B which can load C, and C needs B's key, not A's.
 *
 * @author timfaulkes
 */
public class ThreadLocalKeySaver {
    private static final ThreadLocal<Deque<Key>> threadLocalKeys = ThreadLocal.withInitial(ArrayDeque::new);

    public static void save(Key key) {
        threadLocalKeys.get().addLast(key);
    }

    public static void clear() {
        threadLocalKeys.get().removeLast();
    }

    public static Key get() {
        Deque<Key> keys = threadLocalKeys.get();
        if (keys.isEmpty()) {
            return null;
        }
        return keys.getLast();
    }
}
