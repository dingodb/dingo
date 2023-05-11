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

import com.google.common.base.Objects;
import io.dingodb.client.operation.number.ComputeLong;
import io.dingodb.client.operation.number.ComputeNumber;
import io.dingodb.client.operation.unit.MergedUnit;
import io.dingodb.client.operation.unit.MixedUnit;
import io.dingodb.client.operation.unit.Value;

import java.util.Arrays;
import java.util.Collection;
import java.util.StringJoiner;

public class AvgMixedUnit<N extends ComputeNumber<N>> extends NumberUnit<ComputeLong, AvgMixedUnit<N>>
        implements MixedUnit<AvgMixedUnit<N>>, Value<Number> {

    private CountUnit countUnit;
    private SumUnit<N> sumUnit;

    public AvgMixedUnit() {
    }

    public AvgMixedUnit(ComputeLong count, N sum) {
        this.countUnit = new CountUnit(count);
        this.sumUnit = new SumUnit<>(sum);
    }

    @Override
    public AvgMixedUnit<N> mixMerge(AvgMixedUnit<N> mixUnit) {
        if (mixUnit == null) {
            return this;
        }
        countUnit.merge(mixUnit.countUnit);
        sumUnit.merge(mixUnit.sumUnit);
        return this;
    }

    @Override
    public AvgMixedUnit<N> mixMerge(MergedUnit unit) {
        if (unit instanceof SumUnit) {
            sumUnit.merge((SumUnit) unit);
        }
        if (unit instanceof CountUnit) {
            countUnit.merge((CountUnit) unit);
        }
        return this;
    }

    @Override
    public AvgMixedUnit<N> merge(AvgMixedUnit<N> that) {
        return mixMerge(that);
    }

    @Override
    public Collection<Class<? extends MergedUnit>> supportUnit() {
        return Arrays.asList(CountUnit.class, SumUnit.class);
    }

    @Override
    public AvgMixedUnit<N> fastClone() {
        AvgMixedUnit<N> result = new AvgMixedUnit<>();
        result.countUnit = this.countUnit.fastClone();
        result.sumUnit = this.sumUnit.fastClone();
        return result;
    }

    @Override
    public Number value() {
        return sumUnit.fastClone().divide(countUnit).value();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        AvgMixedUnit<?> that = (AvgMixedUnit<?>) other;
        return Objects.equal(countUnit, that.countUnit) && Objects.equal(sumUnit, that.sumUnit);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(countUnit, sumUnit);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", AvgMixedUnit.class.getSimpleName() + "[", "]")
            .add("countUnit=" + countUnit)
            .add("sumUnit=" + sumUnit)
            .toString();
    }
}
