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

import io.dingodb.client.operation.number.ComputeNumber;

public class MinUnit<N extends ComputeNumber<N>> extends NumberUnit<N, MinUnit<N>> {

    public MinUnit() {
        super(null, 0L);
    }

    public MinUnit(N value) {
        super(value, 1L);
    }

    public MinUnit(N value, long count) {
        super(value, count);
    }

    @Override
    public MinUnit<N> merge(MinUnit<N> that) {
        if (that == null) {
            return this;
        }
        count.add(that.count);
        // setValue(min(value, that.value));
        return this;
    }

    @Override
    public MinUnit<N> fastClone() {
        MinUnit<N> minUnit = new MinUnit<>(value.fastClone(), count.value());
        return minUnit;
    }
}
