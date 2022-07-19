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

public class ComputeDouble implements ComputeNumber<ComputeDouble> {

    private Double value;

    public ComputeDouble() {
    }

    public ComputeDouble(Double value) {
        this.value = value;
    }

    public static ComputeDouble of(Double value) {
        return new ComputeDouble(value == null ? 0.0 : value);
    }

    public static ComputeDouble of(Number value) {
        return new ComputeDouble(value == null ? 0 : value.doubleValue());
    }

    public static ComputeDouble of(ComputeNumber<?> computeNumber) {
        return new ComputeDouble(computeNumber.doubleValue());
    }

    /**
     * Add num.
     */
    @Override
    public ComputeDouble add(ComputeNumber<?> num) {
        value += num.doubleValue();
        return this;
    }

    /**
     * Subtract num.
     */
    @Override
    public ComputeDouble subtract(ComputeNumber<?> num) {
        value -= num.doubleValue();
        return this;
    }

    /**
     * Multiply num.
     */
    @Override
    public ComputeDouble multiply(ComputeNumber<?> num) {
        value *= num.doubleValue();
        return this;
    }

    /**
     * Divide num.
     */
    @Override
    public ComputeDouble divide(ComputeNumber<?> num) {
        value /= num.doubleValue();
        return this;
    }

    /**
     * Remainder num.
     */
    @Override
    public ComputeDouble remainder(ComputeNumber<?> num) {
        value %= num.doubleValue();
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
    public ComputeDouble abs() {
        value = (value <= 0.0D) ? 0.0D - value : value;
        return this;
    }

    /**
     * Negate.
     */
    @Override
    public ComputeDouble negate() {
        value = 0.0D - value;
        return this;
    }

    @Override
    public ComputeDouble fastClone() {
        return new ComputeDouble(value);
    }

    @Override
    public ComputeDouble value(Value value) {
        this.value = value.value().doubleValue();
        return this;
    }

    @Override
    public Value value() {
        return Value.get(value);
    }

    @Override
    public int compareTo(ComputeDouble that) {
        return Double.compare(value, that.doubleValue());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ComputeDouble that = (ComputeDouble) obj;
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
