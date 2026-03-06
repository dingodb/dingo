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

package io.dingodb.common.memory;

import io.dingodb.common.mysql.scope.ScopeVariables;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;

public class GlobalMemoryPool extends BlockingMemoryPool {
    private final List<MemoryPoolListener> listeners = new CopyOnWriteArrayList<>();

    public GlobalMemoryPool(String name, long limit) {
        super(name, limit, null, MemoryType.GLOBAL);
        this.maxElasticMemory = this.maxLimit;
    }

    @Override
    public void requestMemoryRevoke() {
        this.onMemoryReserved();
    }

    public void addListener(MemoryPoolListener listener) {
        listeners.add(requireNonNull(listener, "listener cannot be null"));
    }

    public void removeListener(MemoryPoolListener listener) {
        listeners.remove(requireNonNull(listener, "listener cannot be null"));
    }

    private void onMemoryReserved() {
        listeners
            .forEach(listener -> listener.onMemoryReserved(this, ScopeVariables.getMemoryRevokingTarget()));
    }
}
