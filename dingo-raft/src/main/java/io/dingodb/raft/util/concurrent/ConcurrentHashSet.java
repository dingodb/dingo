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

package io.dingodb.raft.util.concurrent;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class ConcurrentHashSet<E> extends AbstractSet<E> {
    private ConcurrentHashMap<E, Boolean> map;

    /**
     * constructor
     */
    public ConcurrentHashSet() {
        super();
        map = new ConcurrentHashMap<>();
    }

    /**
     * return the size of the map
     * @see java.util.AbstractCollection#size()
     */
    @Override
    public int size() {
        return map.size();
    }

    /**
     *
     * @see java.util.AbstractCollection#contains(Object)
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    @Override
    public boolean contains(Object o) {
        return map.containsKey(o);
    }

    /**
     *
     * @see java.util.AbstractCollection#iterator()
     */
    @Override
    public Iterator<E> iterator() {
        return map.keySet().iterator();
    }

    /**
     * add an obj to set, if exist, return false, else return true
     * @see java.util.AbstractCollection#add(Object)
     */
    @Override
    public boolean add(E o) {
        return map.putIfAbsent(o, Boolean.TRUE) == null;
    }

    /**
     *
     * @see java.util.AbstractCollection#remove(Object)
     */
    @Override
    public boolean remove(Object o) {
        return map.remove(o) != null;
    }

    /**
     * clear the set
     * @see java.util.AbstractCollection#clear()
     */
    @Override
    public void clear() {
        map.clear();
    }
}
