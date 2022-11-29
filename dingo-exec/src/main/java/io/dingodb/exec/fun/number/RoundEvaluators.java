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
    induceSequence = {
        long.class,
        int.class,
        double.class,
    }
)
final class RoundEvaluators {
    private RoundEvaluators() {
    }

    static int round(int value, long scale) {
        if (scale >= 0) {
            return value;
        }
        return round(BigDecimal.valueOf(value), scale).intValue();
    }

    static long round(long value, long scale) {
        if (scale >= 0) {
            return value;
        }
        return round(BigDecimal.valueOf(value), scale).longValue();
    }

    static double round(double value, long scale) {
        return round(BigDecimal.valueOf(value), scale).doubleValue();
    }

    static @NonNull BigDecimal round(@NonNull BigDecimal value, long scale) {
        if (scale > 10 || scale < -10) {
            throw new ArithmeticException("Parameter out of range.");
        }
        BigDecimal result = value.setScale((int) scale, RoundingMode.HALF_UP);
        if (scale < 0) {
            result = result.setScale(0, RoundingMode.HALF_UP);
        }
        return result;
    }

    static int round(int value) {
        return value;
    }

    static long round(long value) {
        return value;
    }

    static double round(double value) {
        return round(value, 0);
    }

    static @NonNull BigDecimal round(@NonNull BigDecimal value) {
        return round(value, 0);
    }
}
