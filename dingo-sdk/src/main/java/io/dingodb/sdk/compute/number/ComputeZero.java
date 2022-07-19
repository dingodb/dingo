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

public class ComputeZero<N extends ComputeNumber<N>> implements ComputeNumber<N> {
    /**
     * Add num.
     */
    @Override
    public N add(ComputeNumber<?> num) {
        return (N) num.fastClone();
    }

    /**
     * Subtract num.
     */
    @Override
    public N subtract(ComputeNumber<?> num) {
        return (N) num.fastClone().negate();
    }

    /**
     * Multiply num.
     */
    @Override
    public N multiply(ComputeNumber<?> num) {
        return (N) this;
    }

    /**
     * Divide num.
     */
    @Override
    public N divide(ComputeNumber<?> num) {
        return (N) this;
    }

    /**
     * Remainder num.
     */
    @Override
    public N remainder(ComputeNumber<?> num) {
        return (N) this;
    }

    /**
     * Set number value.
     */
    @Override
    public N value(Value value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value value() {
        return Value.get(0);
    }

    /**
     * Signum.
     *
     * @return -1, 0, or 1 as the value of this num is negative, zero, or positive.
     */
    @Override
    public int signum() {
        return 0;
    }

    /**
     * Abs.
     */
    @Override
    public N abs() {
        return (N) this;
    }

    /**
     * Negate.
     */
    @Override
    public N negate() {
        return (N) this;
    }

    @Override
    public N fastClone() {
        return (N) new ComputeZero();
    }

    @Override
    public int compareTo(ComputeNumber that) {
        return that.signum();
    }

    @Override
    public String toString() {
        return "0";
    }
}
