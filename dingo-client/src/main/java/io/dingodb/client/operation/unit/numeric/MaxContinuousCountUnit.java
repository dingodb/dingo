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

import java.util.Objects;

public class MaxContinuousCountUnit extends BoundaryUnit<Boolean, MaxContinuousCountUnit> {

    public long headCount = 0L;

    public long tailCount = 0L;

    public MaxContinuousCountUnit() {
    }

    public MaxContinuousCountUnit(Boolean center) {
        this(center, center, ComputeLong.of(center ? 1L : 0L), 1L, center ? 1L : 0L, center ? 1L : 0L);
    }

    public MaxContinuousCountUnit(Boolean head, Boolean tail,
                                  ComputeLong value, long count,
                                  long headCount, long tailCount) {
        super(head, tail, value, count);
        this.headCount = headCount;
        this.tailCount = tailCount;
    }

    private void internalMergeOp(MaxContinuousCountUnit that) {
        long l1 = this.value.longValue();
        long l2 = that.value.longValue();
        long l3 = Math.max(l1, l2);
        long l4 = 0L;
        if (that.headCount == l2) {
            if (that.head && this.tail) {
                l3 = Math.max(l3, that.headCount + this.tailCount);
                if (that.count.value() - that.tailCount <= 0L) {
                    l4 = that.tailCount + this.tailCount;
                }
                if (this.count.value() - this.headCount <= 0L) {
                    this.headCount += that.headCount;
                }
            }
        }
        this.value.value(l3);
        this.tail = that.tail;
        this.tailCount = Math.max(that.tailCount, l4);
        this.count.add(that.count);
    }

    @Override
    public MaxContinuousCountUnit merge(MaxContinuousCountUnit that) {
        internalMergeOp(that);
        return this;
    }

    @Override
    public MaxContinuousCountUnit fastClone() {
        return new MaxContinuousCountUnit(
            this.head, this.tail, this.value, this.count.value(), this.headCount, this.tailCount);
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
        MaxContinuousCountUnit maxContinuousCountNumber = (MaxContinuousCountUnit) other;
        if (this.headCount != maxContinuousCountNumber.headCount) {
            return false;
        }
        if (this.tailCount != maxContinuousCountNumber.tailCount) {
            return false;
        }
        return super.equals(other);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), headCount, tailCount);
    }
}
