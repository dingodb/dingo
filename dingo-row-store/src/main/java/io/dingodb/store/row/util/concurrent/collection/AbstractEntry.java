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

package io.dingodb.store.row.util.concurrent.collection;

import java.util.Map;

/**
 * A simple implementation of {@link Map.Entry}.
 * Does not implement {@link Map.Entry#setValue(Object)}, that is done by users of the class.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 * @author Cliff Click
 * @since 1.5
 *
 * Forked from <a href="https://github.com/JCTools/JCTools">JCTools</a>.
 */
abstract class AbstractEntry<K, V> implements Map.Entry<K, V> {
    protected final K _key;

    protected V       _val;

    public AbstractEntry(final K key, final V val) {
        _key = key;
        _val = val;
    }

    public AbstractEntry(final Map.Entry<K, V> e) {
        _key = e.getKey();
        _val = e.getValue();
    }

    public String toString() {
        return _key + "=" + _val;
    }

    public K getKey() {
        return _key;
    }

    public V getValue() {
        return _val;
    }

    public boolean equals(final Object o) {
        if (!(o instanceof Map.Entry))
            return false;
        final Map.Entry e = (Map.Entry) o;
        return eq(_key, e.getKey()) && eq(_val, e.getValue());
    }

    public int hashCode() {
        return ((_key == null) ? 0 : _key.hashCode()) ^ ((_val == null) ? 0 : _val.hashCode());
    }

    private static boolean eq(final Object o1, final Object o2) {
        return (o1 == null ? o2 == null : o1.equals(o2));
    }
}
