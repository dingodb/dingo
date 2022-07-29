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

package io.dingodb.common.operation.compute.number;

import io.dingodb.common.operation.Value;

import java.util.Objects;

public class ComputeLong implements ComputeNumber<ComputeLong> {

    private Long value;

    public ComputeLong() {
    }

    public ComputeLong(Long value) {
        this.value = value;
    }

    public static ComputeLong of(Long value) {
        return new ComputeLong(value == null ? 0 : value);
    }

    public static ComputeLong of(Number value) {
        return new ComputeLong(value == null ? 0 : value.longValue());
    }

    public static ComputeLong of(ComputeNumber<?> computeNumber) {
        return new ComputeLong(computeNumber.longValue());
    }

    /**
     * Set number value.
     */
    @Override
    public ComputeLong value(Value value) {
        this.value = value.value().longValue();
        return this;
    }

    @Override
    public Value value() {
        return Value.get(value);
    }

    public ComputeLong inc() {
        value++;
        return this;
    }

    public ComputeLong inc(Long inc) {
        value += inc;
        return this;
    }

    /**
     * Add num.
     */
    @Override
    public ComputeLong add(ComputeNumber<?> num) {
        value += num.longValue();
        return this;
    }

    /**
     * Subtract num.
     */
    @Override
    public ComputeLong subtract(ComputeNumber<?> num) {
        value -= num.longValue();
        return this;
    }

    /**
     * Multiply num.
     */
    @Override
    public ComputeLong multiply(ComputeNumber<?> num) {
        value *= num.longValue();
        return this;
    }

    /**
     * Divide num.
     */
    @Override
    public ComputeLong divide(ComputeNumber<?> num) {
        value /= num.longValue();
        return this;
    }

    /**
     * Remainder num.
     */
    @Override
    public ComputeLong remainder(ComputeNumber<?> num) {
        value %= num.longValue();
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
    public ComputeLong abs() {
        value = (value < 0) ? -value : value;
        return this;
    }

    /**
     * Negate.
     */
    @Override
    public ComputeLong negate() {
        value = -value;
        return this;
    }

    @Override
    public ComputeLong fastClone() {
        return new ComputeLong(value);
    }

    @Override
    public int compareTo(ComputeLong that) {
        return Long.compare(value, that.longValue());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ComputeLong that = (ComputeLong) obj;
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
