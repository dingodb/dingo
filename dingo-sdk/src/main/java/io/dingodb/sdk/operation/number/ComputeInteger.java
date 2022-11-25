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

package io.dingodb.sdk.operation.number;

import java.util.Objects;

public class ComputeInteger implements ComputeNumber<ComputeInteger> {

    private Integer value;

    public ComputeInteger() {
    }

    public ComputeInteger(Integer value) {
        this.value = value;
    }

    public static ComputeInteger of(Integer value) {
        return new ComputeInteger(value == null ? 0 : value);
    }

    public static ComputeInteger of(Number value) {
        return new ComputeInteger(value == null ? 0 : value.intValue());
    }

    public static ComputeInteger of(ComputeNumber<?> computeNumber) {
        return new ComputeInteger(computeNumber.intValue());
    }

    /**
     * Set number value.
     */
    @Override
    public ComputeInteger value(Number value) {
        this.value = value.intValue();
        return this;
    }

    @Override
    public Integer value() {
        return value;
    }

    /**
     * Add num.
     */
    @Override
    public ComputeInteger add(ComputeNumber<?> num) {
        value += num.intValue();
        return this;
    }

    /**
     * Subtract num.
     */
    @Override
    public ComputeInteger subtract(ComputeNumber<?> num) {
        value -= num.intValue();
        return this;
    }

    /**
     * Multiply num.
     */
    @Override
    public ComputeInteger multiply(ComputeNumber<?> num) {
        value *= num.intValue();
        return this;
    }

    /**
     * Divide num.
     */
    @Override
    public ComputeInteger divide(ComputeNumber<?> num) {
        value /= num.intValue();
        return this;
    }

    /**
     * Remainder num.
     */
    @Override
    public ComputeInteger remainder(ComputeNumber<?> num) {
        value %= num.intValue();
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
    public ComputeInteger abs() {
        value = (value < 0) ? -value : value;
        return this;
    }

    /**
     * Negate.
     */
    @Override
    public ComputeInteger negate() {
        value = -value;
        return this;
    }

    @Override
    public ComputeInteger fastClone() {
        return new ComputeInteger(value);
    }

    @Override
    public int compareTo(ComputeInteger that) {
        return Integer.compare(value, that.intValue());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ComputeInteger that = (ComputeInteger) obj;
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
