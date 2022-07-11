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

package io.dingodb.sdk.compute.number;

import io.dingodb.sdk.common.Value;

import java.util.Objects;

public class ComputeFloat implements ComputeNumber<ComputeFloat> {

    private Float value;

    public ComputeFloat() {
    }

    public ComputeFloat(Float value) {
        this.value = value;
    }

    public static ComputeFloat of(Float value) {
        return new ComputeFloat(value == null ? 0.0F : value);
    }

    public static ComputeFloat of(Number value) {
        return new ComputeFloat(value == null ? 0 : value.floatValue());
    }

    public static ComputeFloat of(ComputeNumber<?> computeNumber) {
        return new ComputeFloat(computeNumber.floatValue());
    }

    /**
     * Add num.
     */
    @Override
    public ComputeFloat add(ComputeNumber<?> num) {
        value += num.floatValue();
        return this;
    }

    /**
     * Subtract num.
     */
    @Override
    public ComputeFloat subtract(ComputeNumber<?> num) {
        value -= num.floatValue();
        return this;
    }

    /**
     * Multiply num.
     */
    @Override
    public ComputeFloat multiply(ComputeNumber<?> num) {
        value *= num.floatValue();
        return this;
    }

    /**
     * Divide num.
     */
    @Override
    public ComputeFloat divide(ComputeNumber<?> num) {
        value /= num.floatValue();
        return this;
    }

    /**
     * Remainder num.
     */
    @Override
    public ComputeFloat remainder(ComputeNumber<?> num) {
        value %= num.floatValue();
        return this;
    }

    /**
     * Signum.
     *
     * @return -1, 0, or 1 as the value of this num is negative, zero, or positive.
     */
    @Override
    public int signum() {
        return value > 0 ? 1 : value == 0 ? 0 : -1;
    }

    /**
     * Abs.
     */
    @Override
    public ComputeFloat abs() {
        value = (value <= 0.0F) ? 0.0F - value : value;
        return this;
    }

    /**
     * Negate.
     */
    @Override
    public ComputeFloat negate() {
        value = 0.0F - value;
        return this;
    }

    @Override
    public ComputeFloat fastClone() {
        return new ComputeFloat(value);
    }

    @Override
    public ComputeFloat value(Value value) {
        this.value = value.value().floatValue();
        return this;
    }

    @Override
    public Value value() {
        return Value.get(value);
    }

    @Override
    public int compareTo(ComputeFloat o) {
        return Float.compare(value, o.floatValue());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ComputeFloat that = (ComputeFloat) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
