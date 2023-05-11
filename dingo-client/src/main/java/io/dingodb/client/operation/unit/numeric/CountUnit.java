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

public class CountUnit extends NumberUnit<ComputeLong, CountUnit> {

    public CountUnit() {
        super();
    }

    public CountUnit(ComputeNumber value) {
        this(value instanceof ComputeLong ? (ComputeLong) value : ComputeLong.of(value));
    }

    public CountUnit(ComputeLong value) {
        super(value, value.fastClone());
    }

    public CountUnit(ComputeNumber value, long count) {
        super(value instanceof ComputeLong ? (ComputeLong) value : ComputeLong.of(value), count);
    }

    @Override
    public Number value() {
        return (this.value == null) ? 0L : this.value.longValue();
    }

    @Override
    public CountUnit merge(CountUnit that) {
        if (that == null) {
            return this;
        }
        value.add(that.value);
        count.add(that.count);
        return this;
    }

    @Override
    public CountUnit fastClone() {
        return new CountUnit(value.fastClone(), count.longValue());
    }
}
