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
import io.dingodb.client.operation.unit.MergedUnit;
import io.dingodb.client.operation.unit.Value;

import java.util.Objects;

public abstract class NumberUnit<N extends ComputeNumber<N>, M extends NumberUnit<N, M>>
    implements ComputeNumber<M>, MergedUnit<M>, Value<Number> {

    private static final long serialVersionUID = -3482087635114157005L;

    protected ComputeLong count;
    protected N value;

    public NumberUnit() {
        this(null, 0);
    }

    public NumberUnit(N value) {
        this(value, 1);
    }

    public NumberUnit(N value, long count) {
        this(value, ComputeLong.of(count));
    }

    public NumberUnit(N value, ComputeLong count) {
        this.value = value;
        this.count = count;
    }

    public ComputeLong getCount() {
        return count;
    }

    public NumberUnit setCount(ComputeLong count) {
        this.count = count;
        return this;
    }

    public N getValue() {
        return value;
    }

    public NumberUnit setValue(N value) {
        this.value = value;
        return this;
    }

    @Override
    public int intValue() {
        return (value.value() == null) ? 0 : value.intValue();
    }

    @Override
    public long longValue() {
        return (value.value() == null) ? 0L : value.longValue();
    }

    @Override
    public float floatValue() {
        return (value. value() == null) ? 0.0F : value.floatValue();
    }

    @Override
    public double doubleValue() {
        return (value.value() == null) ? 0.0D : value.doubleValue();
    }

    @Override
    public byte byteValue() {
        return (value.value() == null) ? 0 : value.byteValue();
    }

    @Override
    public short shortValue() {
        return (value.value() == null) ? 0 : value.shortValue();
    }

    @Override
    public M add(ComputeNumber<?> num) {
        value.add(num);
        return (M) this;
    }

    @Override
    public M subtract(ComputeNumber<?> num) {
        value.subtract(num);
        return (M) this;
    }

    @Override
    public M multiply(ComputeNumber<?> num) {
        value.multiply(num);
        return (M) this;
    }

    @Override
    public M divide(ComputeNumber<?> num) {
        value.divide(num);
        return (M) this;
    }

    @Override
    public M remainder(ComputeNumber<?> num) {
        value.remainder(num);
        return (M) this;
    }

    @Override
    public M merge(M that) {
        return this.merge(that);
    }

    @Override
    public M value(Number value) {
        throw new UnsupportedOperationException(getClass().getCanonicalName() + " not Support add.");
    }

    @Override
    public Number value() {
        return this.value.value();
    }

    @Override
    public int signum() {
        return value.signum();
    }

    @Override
    public M abs() {
        value.abs();
        return (M) this;
    }

    @Override
    public M negate() {
        value.negate();
        return (M) this;
    }

    @Override
    public int hashCode() {
        byte b = 31;
        int i = 1;
        i = b * i + this.count.intValue();
        return b * i + (new Double(doubleValue())).hashCode();
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
        NumberUnit otherUnit = (NumberUnit) other;
        if (!Objects.equals(this.count, otherUnit.count)) {
            return false;
        }
        return Objects.equals(value, otherUnit.value);
    }

    @Override
    public int compareTo(M that) {
        return value.compareTo(that.value);
    }
}
