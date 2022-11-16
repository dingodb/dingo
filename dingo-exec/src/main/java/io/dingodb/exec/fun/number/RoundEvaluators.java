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

package io.dingodb.exec.fun.number;

import io.dingodb.expr.annotations.Evaluators;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Evaluators(
    induceSequence = {BigDecimal.class, double.class, long.class, int.class}
)
public final class RoundEvaluators {
    public static final String NAME = "round";

    private RoundEvaluators() {
    }

    static BigDecimal round(@NonNull BigDecimal value, long scale) {
        if (scale > 10 || scale < -10) {
            throw new ArithmeticException("Parameter out of range.");
        }

        // If value is integer and scale > 0, return the original value
        if (scale > 0 && value.scale() == 0) {
            return value;
        }
        if (scale >= 0) {
            return value.setScale((int) scale, RoundingMode.HALF_UP);
        }

        long temp = 1;
        for (int i = 1; i <= (-scale); i++) {
            temp = temp * 10;
        }

        BigDecimal t = BigDecimal.valueOf(temp);
        BigDecimal divide = value.divide(t);
        if (divide.abs().compareTo(new BigDecimal("0.1")) < 0) {
            return BigDecimal.ZERO;
        }

        divide = divide.setScale(0, RoundingMode.HALF_UP);
        return divide.multiply(t);
    }

    static @NonNull BigDecimal round(@NonNull BigDecimal value) {
        return round(value, 0);
    }
}
