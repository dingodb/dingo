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

package io.dingodb.sdk.operation.unit.numeric;

import io.dingodb.sdk.operation.number.ComputeLong;
import io.dingodb.sdk.operation.number.ComputeNumber;

public class MaxIncreaseCountUnit extends IncreaseCountUnit<MaxIncreaseCountUnit> {

    public long headCount = 0L;

    public long tailCount = 0L;

    public MaxIncreaseCountUnit() {
    }

    public MaxIncreaseCountUnit(ComputeNumber center) {
        super(center);
    }

    public MaxIncreaseCountUnit(ComputeNumber head, ComputeNumber tail,
                                ComputeLong value, long count,
                                long headCount, long tailCount) {
        super(head, tail, value, count);
        this.headCount = headCount;
        this.tailCount = tailCount;
    }

    private void internalMergeOp(MaxIncreaseCountUnit that) {
        ComputeLong thatValue = that.value;
        value = ComputeNumber.max(value, thatValue);
        long tailCount = 0L;
        if (that.headCount == thatValue.value()) {
            if (that.head.compareTo(this.tail) > 0) {
                value.value(Math.max(value.longValue(), that.headCount + this.tailCount + 1L));
                if (that.count.value() - that.tailCount <= 1L) {
                    tailCount = that.tailCount + this.tailCount + 1L;
                }
                if (this.count.value() - this.headCount <= 1L) {
                    this.headCount = this.headCount + that.headCount + 1L;
                }
            }
        }
        this.tail = that.tail;
        this.tailCount = Math.max(tailCount, that.tailCount);
        this.count.add(that.count);
    }

    @Override
    public MaxIncreaseCountUnit merge(MaxIncreaseCountUnit that) {
        if (that == null) {
            return this;
        }
        internalMergeOp(that);
        return this;
    }

    @Override
    public MaxIncreaseCountUnit fastClone() {
        return new MaxIncreaseCountUnit(
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
        MaxIncreaseCountUnit maxIncreaseCountNumber = (MaxIncreaseCountUnit) other;
        if (this.headCount != maxIncreaseCountNumber.headCount) {
            return false;
        }
        if (this.tailCount != maxIncreaseCountNumber.tailCount) {
            return false;
        }
        return super.equals(other);
    }
}
