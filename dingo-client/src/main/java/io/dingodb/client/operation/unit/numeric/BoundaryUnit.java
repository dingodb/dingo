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

package io.dingodb.client.operation.unit.numeric;

import io.dingodb.client.operation.number.ComputeLong;

public abstract class BoundaryUnit<T, M extends NumberUnit<ComputeLong, M>> extends NumberUnit<ComputeLong, M> {

    public T head;

    public T tail;

    public BoundaryUnit() {

    }

    public BoundaryUnit(T center) {
        this(center, center, ComputeLong.of(0L), 1L);
    }

    public BoundaryUnit(T head, T tail, ComputeLong value, long count) {
        super(value, count);
        setHead(head);
        setTail(tail);
    }

    public T getHead() {
        return head;
    }

    public void setHead(T head) {
        this.head = head;
    }

    public T getTail() {
        return tail;
    }

    public void setTail(T tail) {
        this.tail = tail;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (getClass() != other.getClass()) {
            return false;
        }
        BoundaryUnit<T, M> boundaryUnit = (BoundaryUnit<T, M>) other;
        if (this.head == null) {
            if (boundaryUnit.head != null) {
                return false;
            }
        } else if (!this.head.equals(boundaryUnit.head)) {
            return false;
        }
        if (this.tail == null) {
            if (boundaryUnit.tail != null) {
                return false;
            }
        } else if (!this.tail.equals(boundaryUnit.tail)) {
            return false;
        }
        return super.equals(other);
    }

}
