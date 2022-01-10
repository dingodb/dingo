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

package io.dingodb.dingokv;

import io.dingodb.dingokv.util.Maps;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class StateListenerContainer<K> {
    private final ConcurrentMap<K, List<StateListener>> stateListeners = Maps.newConcurrentMap();

    public boolean addStateListener(final K id, final StateListener listener) {
        List<StateListener> group = this.stateListeners.get(id);
        if (group == null) {
            final List<StateListener> newGroup = new CopyOnWriteArrayList<>();
            group = this.stateListeners.putIfAbsent(id, newGroup);
            if (group == null) {
                group = newGroup;
            }
        }
        return group.add(listener);
    }

    public List<StateListener> getStateListenerGroup(final K id) {
        final List<StateListener> group = this.stateListeners.get(id);
        return group == null ? Collections.emptyList() : group;
    }

    public boolean removeStateListener(final K id, final StateListener listener) {
        final List<StateListener> group = this.stateListeners.get(id);
        if (group == null) {
            return false;
        }
        return group.remove(listener);
    }

    public void clear() {
        this.stateListeners.clear();
    }
}
