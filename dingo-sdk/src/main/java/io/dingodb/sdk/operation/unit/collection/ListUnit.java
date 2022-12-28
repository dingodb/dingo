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

import io.dingodb.sdk.operation.Row;
import io.dingodb.sdk.operation.unit.CollectionUnit;
import io.dingodb.sdk.operation.unit.UnlimitedMergedUnit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ListUnit<T> implements UnlimitedMergedUnit<ListUnit<T>>, CollectionUnit<T, ListUnit<T>>, Serializable {

    private List<T> values = new ArrayList<>();

    public int limit = 0;

    public ListUnit() {
    }

    public ListUnit(T value) {
        this();
        add(value);
    }

    public ListUnit(T value, int limit) {
        this();
        add(value);
        this.limit = limit;
    }

    public List<T> getList() {
        return this.values;
    }

    public void setList(List<T> paramList) {
        this.values = paramList;
    }

    @Override
    public ListUnit<T> add(T value) {
        this.values.add(value);
        if (this.limit > 0 && this.values.size() > this.limit) {
            this.values.remove(0);
        }
        return this;
    }

    @Override
    public ListUnit<T> merge(ListUnit<T> that) {
        return merge(that, false);
    }

    private ListUnit<T> merge(ListUnit<T> that, boolean useLimit) {
        if (that == null) {
            return this;
        }
        List<T> values = that.values;
        int limit = that.limit;
        return internalMerge(values, useLimit, limit);
    }

    private ListUnit<T> internalMerge(List<T> values, boolean useLimit, int limit) {
        this.values.addAll(values);
        if (!useLimit) {
            this.limit = Math.max(this.limit, limit);
            int i = this.values.size();
            if (this.limit > 0 && i > this.limit) {
                List<T> list = this.values.subList(i - this.limit, i);
                this.values = new ArrayList<>(this.limit);
                this.values.addAll(list);
            }
        }
        return this;
    }

    @Override
    public ListUnit<T> unlimitedMerge(ListUnit<T> that) {
        return merge(that, true);
    }

    @Override
    public Iterator<Object[]> iterator() {
        return this.values.stream().map(r -> ((Row) r).getRecords()).iterator();
    }

    @Override
    public List<Object[]> value() {
        return this.values.stream().map(r -> ((Row) r).getRecords()).collect(Collectors.toList());
    }

    @Override
    public ListUnit<T> fastClone() {
        ListUnit<T> mergeableListObject = new ListUnit<>();
        mergeableListObject.limit = this.limit;
        for (T item : getList()) {
            mergeableListObject.getList().add(item);
        }
        return mergeableListObject;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that == null || getClass() != that.getClass()) {
            return false;
        }
        ListUnit<T> thatUnit = (ListUnit)that;
        if (this.values == null) {
            if (thatUnit.values != null) {
                return false;
            }
        } else if (!this.values.equals(thatUnit.values)) {
            return false;
        }
        return this.limit == thatUnit.limit;
    }

    @Override
    public String toString() {
        return String.format("{limit=%d, list=%s}", this.limit, this.values.toString());
    }
}
