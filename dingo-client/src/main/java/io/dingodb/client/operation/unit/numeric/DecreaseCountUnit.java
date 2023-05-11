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
import io.dingodb.client.operation.number.ComputeNumber;

public class DecreaseCountUnit<M extends DecreaseCountUnit<M>> extends BoundaryUnit<ComputeNumber, M> {

    public DecreaseCountUnit() {
    }

    public DecreaseCountUnit(ComputeNumber center) {
        super(center);
    }

    public DecreaseCountUnit(ComputeNumber head, ComputeNumber tail, ComputeLong value, long count) {
        super(head, tail, value, count);
    }

    @Override
    public M merge(M that) {
        if (that == null) {
            return (M) this;
        }
        this.value = value
            .add(that.value)
            .add(tail.compareTo(that.head) > 0 ? ComputeLong.of(1L) : ComputeLong.of(0L));
        this.tail = that.tail;
        this.count.add(that.count);
        return (M) this;
    }

    @Override
    public M fastClone() {
        return (M) new DecreaseCountUnit(
            this.head, this.tail, this.value, count.value()
        );
    }
}
