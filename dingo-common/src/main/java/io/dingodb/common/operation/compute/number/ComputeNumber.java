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
import io.dingodb.common.operation.compute.Cloneable;

import java.io.Serializable;

public interface ComputeNumber<N extends ComputeNumber<N>> extends Cloneable<N>, Comparable<N>, Serializable {

    /**
     * Add num.
     */
    N add(ComputeNumber<?> num);

    /**
     * Subtract num.
     */
    N subtract(ComputeNumber<?> num);

    /**
     * Multiply num.
     */
    N multiply(ComputeNumber<?> num);

    /**
     * Divide num.
     */
    N divide(ComputeNumber<?> num);

    /**
     * Remainder num.
     */
    N remainder(ComputeNumber<?> num);

    /**
     * Set number value.
     */
    N value(Value value);

    Value value();

    /**
     * Signum.
     *
     * @return -1, 0, or 1 as the value of this num is negative, zero, or positive.
     */
    int signum();

    /**
     * Abs.
     */
    N abs();

    /**
     * Negate.
     */
    N negate();

    @Override
    N fastClone();

    /**
     * Return int value.
     */
    default int intValue() {
        return value().value().intValue();
    }

    /**
     * Return long value.
     */
    default long longValue() {
        return value().value().longValue();
    }

    /**
     * Return float value.
     */
    default float floatValue() {
        return value().value().floatValue();
    }

    /**
     * Return double value.
     */
    default double doubleValue() {
        return value().value().doubleValue();
    }

    /**
     * Return byte value.
     */
    default byte byteValue() {
        return value().value().byteValue();
    }

    /**
     * Return short value.
     */
    default short shortValue() {
        return value().value().shortValue();
    }

    /**
     * Return number for cls.
     */
    default <N extends ComputeNumber<N>> N cast(Class<N> cls) throws Exception {
        return cls.newInstance().value(this.value());
    }

    static <N extends ComputeNumber<N>> N max(N n1, N n2) {
        return n1.compareTo(n2) > 0 ? n1 : n2;
    }

    static <N extends ComputeNumber<N>> N min(N n1, N n2) {
        return n1.compareTo(n2) < 0 ? n1 : n2;
    }

}
