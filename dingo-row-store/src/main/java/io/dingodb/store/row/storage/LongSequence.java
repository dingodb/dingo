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

package io.dingodb.store.row.storage;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public abstract class LongSequence {
    private final long base;

    private Sequence sequence;
    private long value;

    public LongSequence() {
        this(0);
    }

    public LongSequence(long base) {
        this.base = base;
    }

    public synchronized long get() {
        return this.base + this.value;
    }

    public synchronized long next() {
        if (this.sequence == null) {
            reset();
        }
        final long val = this.value++;
        if (val == this.sequence.getEndValue()) {
            reset();
            return this.base + this.value++;
        }
        return this.base + val;
    }

    private void reset() {
        this.sequence = getNextSequence();
        this.value = sequence.getStartValue();
    }

    public abstract Sequence getNextSequence();
}
