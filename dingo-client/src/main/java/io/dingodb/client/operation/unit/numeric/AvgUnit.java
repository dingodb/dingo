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

public class AvgUnit<N extends ComputeNumber<N>> extends NumberUnit<N, AvgUnit<N>> {
    private static final long serialVersionUID = -8025355452458424095L;

    public AvgUnit() {
        super();
    }

    public AvgUnit(N value) {
        super(value, (value == null) ? ComputeLong.of(0L) : ComputeLong.of(1L));
    }

    public AvgUnit(N value, long count) {
        super(value, count);
    }

    @Override
    public AvgUnit<N> merge(AvgUnit<N> that) {
        if (that == null) {
            return this;
        }
        value.multiply(count).add(that.value.multiply(that.count)).divide(count.add(that.count));
        return this;
    }

    @Override
    public AvgUnit<N> fastClone() {
        return new AvgUnit<>(value.fastClone(), count.longValue());
    }
}
