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

package io.dingodb.sdk.operation.unit.collection;

import io.dingodb.sdk.operation.unit.CollectionUnit;
import io.dingodb.sdk.operation.unit.UnlimitedMergedUnit;
import io.dingodb.sdk.operation.unit.Value;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DistinctListUnit<T extends Object> implements CollectionUnit<T, DistinctListUnit<T>>,
    UnlimitedMergedUnit<DistinctListUnit<T>>, Value<Collection<T>>, Serializable, Iterable<T> {

    private Set<T> original = new HashSet<>();

    private int limit = 0;

    public DistinctListUnit() {
    }

    public DistinctListUnit(T value) {
        this();
        add(value);
    }

    public DistinctListUnit(Collection<T> values) {
        this();
        addAll(values);
    }

    /**
     * Construct.
     *
     * @param value input
     * @param limit    limitCnt
     */
    public DistinctListUnit(T value, int limit) {
        this();
        add(value);
        this.limit = limit;
    }

    /**
     * Construct.
     */
    public DistinctListUnit(Collection<T> values, int paramCnt) {
        this();
        addAll(values);
        this.limit = paramCnt;
    }

    public Set<T> original() {
        return original;
    }

    public int limit() {
        return limit;
    }

    @Override
    public DistinctListUnit<T> merge(DistinctListUnit<T> that) {
        return internalMergeOp(that, false);
    }

    @Override
    public DistinctListUnit<T> unlimitedMerge(DistinctListUnit<T> that) {
        return internalMergeOp(that, true);
    }

    private DistinctListUnit<T> internalMergeOp(DistinctListUnit<T> that, boolean hasLimit) {
        if (that == null) {
            return this;
        }
        return originalMerge(that.original, that.limit, hasLimit);
    }

    private DistinctListUnit<T> originalMerge(Set<T> original, int limit, boolean hasLimit) {
        this.original.addAll(original);
        if (!hasLimit) {
            this.limit = Math.max(this.limit, limit);
            int i = this.original.size();
            if (this.limit > 0 && i > this.limit) {
                byte b = 0;
                HashSet<T> hashSet = new HashSet<>();
                int j = i - this.limit;
                for (T item : this.original) {
                    if (b++ < j) {
                        continue;
                    }
                    hashSet.add(item);
                }
                this.original = hashSet;
            }
        }
        return this;
    }

    @Override
    public Collection<T> value() {
        return this.original;
    }

    public Collection<T> asCollection() {
        return this.original;
    }

    /**
     * Add value to original.
     * @param value input param
     * @return
     */
    @Override
    public DistinctListUnit<T> add(T value) {
        this.original.add(value);
        if (this.limit > 0 && this.original.size() > this.limit) {
            this.original.remove(this.original.iterator().next());
        }
        return this;
    }

    /**
     * Add values to original.
     */
    public void addAll(Collection<T> values) {
        this.original.addAll(values);
        while (this.limit > 0 && this.original.size() > this.limit) {
            this.original.remove(this.original.iterator().next());
        }
    }

    @Override
    public Iterator<T> iterator() {
        return this.original.iterator();
    }

    @Override
    public String toString() {
        return String.format("{limit=%d, set=%s}", this.limit, this.original.toString());
    }

    @Override
    public DistinctListUnit<T> fastClone() {
        DistinctListUnit<T> distinctListUnit = new DistinctListUnit<>();
        distinctListUnit.limit = this.limit;
        for (T item : getSet()) {
            distinctListUnit.getSet().add(item);
        }
        return distinctListUnit;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that == null) {
            return false;
        }
        if (getClass() != that.getClass()) {
            return false;
        }
        DistinctListUnit<T> thatUnit = (DistinctListUnit) that;
        if (this.limit != thatUnit.limit) {
            return false;
        }
        if (this.original == null) {
            return thatUnit.original == null;
        } else {
            return this.original.equals(thatUnit.original);
        }
    }

    public Set<T> getSet() {
        return this.original;
    }

    public void setSet(Set<T> distinctHashRs) {
        this.original = distinctHashRs;
    }
}
