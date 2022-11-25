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

import io.dingodb.sdk.operation.number.ComputeNumber;

public class SumUnit<N extends ComputeNumber<N>> extends NumberUnit<N, SumUnit<N>> {
    private static final long serialVersionUID = -5080748154206926705L;

    public SumUnit() {
        super(null, 0L);
    }

    public SumUnit(N value) {
        super(value, (value == null) ? 0L : ((value.doubleValue() < 0.0D) ? -1 : 1));
    }

    public SumUnit(N value, long count) {
        super(value, count);
    }

    @Override
    public SumUnit<N> merge(SumUnit<N> that) {
        if (that == null) {
            return this;
        }
        count.add(that.count);
        value.add(that.value);
        return this;
    }

    @Override
    public SumUnit<N> fastClone() {
        return new SumUnit<>(value.fastClone(), count.longValue());
    }
}
