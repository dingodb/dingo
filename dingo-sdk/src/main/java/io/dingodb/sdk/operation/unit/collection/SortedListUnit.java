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

import io.dingodb.sdk.operation.Cloneable;
import io.dingodb.sdk.operation.Row;
import io.dingodb.sdk.operation.unit.CollectionUnit;
import io.dingodb.sdk.operation.unit.Value;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class SortedListUnit<T extends Comparable<T> & Cloneable<T>>
        implements CollectionUnit<T, SortedListUnit<T>>, Serializable {

    public boolean desc = true;

    public int limit = 0;

    private List<T> original = new ArrayList<>();

    public SortedListUnit(T value) {
        this(value, 0, true);
    }

    /**
     * Constructor.
     * @param value  value
     * @param limit list limit
     * @param desc des or not
     */
    public SortedListUnit(T value, int limit, boolean desc) {
        this();
        this.limit = limit;
        this.desc = desc;
        add(value);
    }

    public SortedListUnit() {
    }

    public SortedListUnit(T value, boolean desc) {
        this(value, 0, desc);
    }

    public SortedListUnit(T value, int limit) {
        this(value, limit, true);
    }

    public int limit() {
        return limit;
    }

    public List<T> original() {
        return original;
    }

    public boolean desc() {
        return desc;
    }

    /**
     * add element.
     * @param value value
     * @return
     */
    @Override
    public SortedListUnit<T> add(T value) {
        if (this.original.isEmpty()) {
            this.original.add(value);
            return this;
        }
        int i = 0;
        int j = this.original.size();
        while (j - i > 1) {
            int k = (i + j) / 2;
            Comparable<T> comparable = this.original.get(k);
            int m = comparable.compareTo(value);
            if ((!this.desc && m > 0) || (this.desc && m < 0)) {
                j = k;
                continue;
            }
            if ((!this.desc && m < 0) || (this.desc && m > 0)) {
                i = k;
                continue;
            }
            i = k;
        }
        if ((this.desc && this.original.get(i).compareTo(value) <= 0)
            || (!this.desc && this.original.get(i).compareTo(value) >= 0)) {
            this.original.add(i, value);
        } else {
            this.original.add(i + 1, value);
        }
        if (this.limit > 0 && this.original.size() > this.limit) {
            this.original.remove(this.original.size() - 1);
        }
        return this;
    }

    @Override
    public SortedListUnit<T> merge(SortedListUnit<T> that) {
        return that == null ? this : internalMerge(that.desc(), that.limit(), that.original());
    }

    private SortedListUnit<T> internalMerge(boolean desc, int limit, List<T> original) {
        this.desc = desc;
        this.limit = Math.max(this.limit, limit);
        ArrayList<T> arrayList = new ArrayList();
        byte b1 = 0;
        byte b2 = 0;
        int i = this.original.size();
        int j = original.size();
        while (b1 < i || b2 < j) {
            T c1 = null;
            T c2 = null;
            if (b1 < i) {
                c1 = this.original.get(b1);
            }
            if (b2 < j) {
                c2 = original.get(b2);
            }
            if (c2 != null && c1 != null) {
                if ((this.desc && c1.compareTo(c2) >= 0) || (!this.desc && c1.compareTo(c2) <= 0)) {
                    arrayList.add(c1);
                    b1++;
                    continue;
                }
                arrayList.add(c2);
                b2++;
                continue;
            }
            if (c2 != null) {
                arrayList.add(c2);
                b2++;
                continue;
            }
            if (c1 != null) {
                arrayList.add(c1);
                b1++;
            }
        }
        if (this.limit > 0 && arrayList.size() > this.limit) {
            ArrayList<T> arrayList1 = new ArrayList<>(this.limit);
            arrayList1.addAll(arrayList.subList(0, this.limit));
            this.original = arrayList1;
        } else {
            this.original = arrayList;
        }
        return this;
    }

    @Override
    public SortedListUnit<T> fastClone() {
        SortedListUnit<T> mergeableSortedList = new SortedListUnit<>();
        mergeableSortedList.desc = this.desc;
        mergeableSortedList.limit = this.limit;
        for (T item : getList()) {
            mergeableSortedList.getList().add(item.fastClone());
        }
        return mergeableSortedList;
    }

    public List<T> getList() {
        return this.original;
    }

    public void setList(ArrayList<T> paramArrayList) {
        this.original = paramArrayList;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return this.original.stream().map(row -> ((Row) row).getRecords()).iterator();
    }

    /**
     * Get median of the list.
     */
    public T getMedian() {
        int i = original.size();
        if (i == 0) {
            return null;
        }
        return original.get(i / 2);
    }

    @Override
    public List<Object[]> value() {
        return this.original.stream().map(row -> ((Row) row).getRecords()).collect(Collectors.toList());
    }

    /**
     * IsEqual or Not.
     * @param that param
     * @return ture or false
     */
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
        SortedListUnit<T> thatUnit = (SortedListUnit) that;
        if (this.desc != thatUnit.desc) {
            return false;
        }
        if (this.limit != thatUnit.limit) {
            return false;
        }
        if (this.original == null) {
            return thatUnit.original == null;
        } else {
            return this.original.equals(thatUnit.original);
        }
    }

    /**
     * toString.
     * @return String
     */
    @Override
    public String toString() {
        return String.format("%s{limit=%s, desc=%s, list=%s}", getClass().getSimpleName(), limit, desc, original);
    }
}
