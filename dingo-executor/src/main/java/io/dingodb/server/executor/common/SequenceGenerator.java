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

package io.dingodb.server.executor.common;

import io.dingodb.common.sequence.SequenceDefinition;

import java.util.concurrent.ConcurrentLinkedQueue;

public class SequenceGenerator {

    private final int increment;
    private final Long minvalue;
    private final Long maxvalue;
    private Long currentValue;
    private final int cache;
    private final boolean cycle;
    private final ConcurrentLinkedQueue<Long> queue;

    public SequenceGenerator(SequenceDefinition definition) {
        if (definition.getMinvalue() > definition.getMaxvalue()) {
            throw new IllegalArgumentException("minvalue cannot be greater than maxvalue.");
        }
        if (definition.getStart() < definition.getMinvalue() || definition.getStart() > definition.getMaxvalue()) {
            throw new IllegalArgumentException("startValue must be within the range of minvalue and maxvalue");
        }
        if (definition.getIncrement() <= 0) {
            throw new IllegalArgumentException("increment must be greater than 0");
        }
        if (definition.getCache() <= 0) {
            throw new IllegalArgumentException("Cache size must be greater than 0");
        }
        this.increment = definition.getIncrement();
        this.minvalue = definition.getMinvalue();
        this.maxvalue = definition.getMaxvalue();
        this.currentValue = definition.getStart();
        this.cache = definition.getCache();
        this.cycle = definition.isCycle();
        this.queue = new ConcurrentLinkedQueue<>();
    }

    private synchronized void fillQueue() {
        while (queue.size() < cache) {
            if (currentValue > maxvalue) {
                if (cycle) {
                    currentValue = minvalue;
                } else {
                    break;
                }
            }
            queue.add(currentValue);
            currentValue += increment;
        }
    }

    public synchronized Long next() {
        if (queue.isEmpty()) {
            // Replenish when cache is empty
            fillQueue();
        }
        return queue.poll();
    }

    public ConcurrentLinkedQueue<Long> getQueue() {
        return queue;
    }

}
