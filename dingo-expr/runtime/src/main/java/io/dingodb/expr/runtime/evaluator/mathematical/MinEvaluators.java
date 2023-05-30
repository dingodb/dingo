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

package io.dingodb.expr.runtime.evaluator.mathematical;

import io.dingodb.expr.annotations.Evaluators;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Evaluators(
    induceSequence = {
        BigDecimal.class,
        double.class,
        long.class,
        int.class,
        boolean.class
    }
)
final class MinEvaluators {
    private MinEvaluators() {
    }

    static int min(int value0, int value1) {
        return Math.min(value0, value1);
    }

    static long min(long value0, long value1) {
        return Math.min(value0, value1);
    }

    static double min(double value0, double value1) {
        return Math.min(value0, value1);
    }

    static boolean min(boolean value0, boolean value1) {
        if (value0 && value1) {
            return value0;
        } else if (value0 && !value1){
            return value1;
        } else if (!value0 && value1) {
            return value0;
        } else {
            return value1;
        }
    }

    static @NonNull BigDecimal min(@NonNull BigDecimal value0, @NonNull BigDecimal value1) {
        return value0.compareTo(value1) <= 0 ? value0 : value1;
    }

    static @NonNull String min(@NonNull String value0, @NonNull String value1) {
        return value0.compareTo(value1) <= 0 ? value0 : value1;
    }

    static @NonNull Date min(@NonNull Date value0, @NonNull Date value1) {
        return value0.compareTo(value1) <= 0 ? value0 : value1;
    }

    static @NonNull Time min(@NonNull Time value0, @NonNull Time value1) {
        return value0.compareTo(value1) <= 0 ? value0 : value1;
    }

    static @NonNull Timestamp min(@NonNull Timestamp value0, @NonNull Timestamp value1) {
        return value0.compareTo(value1) < 0 ? value0 : value1;
    }
}
