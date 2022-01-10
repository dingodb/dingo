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

package io.dingodb.dingokv.client;

import io.dingodb.dingokv.util.Maps;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class RoundRobinLoadBalancer implements LoadBalancer {
    private static final ConcurrentMap<String, RoundRobinLoadBalancer> container = Maps.newConcurrentMap();

    private static final AtomicIntegerFieldUpdater<RoundRobinLoadBalancer> indexUpdater
        = AtomicIntegerFieldUpdater.newUpdater(RoundRobinLoadBalancer.class, "index");

    @SuppressWarnings("unused")
    private volatile int index = 0;

    public static RoundRobinLoadBalancer getInstance(final String regionId) {
        RoundRobinLoadBalancer instance = container.get(regionId);
        if (instance == null) {
            RoundRobinLoadBalancer newInstance = new RoundRobinLoadBalancer();
            instance = container.putIfAbsent(regionId, newInstance);
            if (instance == null) {
                instance = newInstance;
            }
        }
        return instance;
    }

    @Override
    public <T> T select(final List<T> elements) {
        if (elements == null) {
            throw new NullPointerException("elements");
        }

        final int size = elements.size();

        if (size == 1) {
            return elements.get(0);
        }

        final int roundRobinIndex = indexUpdater.getAndIncrement(this) & Integer.MAX_VALUE;

        return elements.get(roundRobinIndex % size);
    }
}
