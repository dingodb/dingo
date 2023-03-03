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

package io.dingodb.expr.runtime.evaluator.relational;

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
        float.class,
        long.class,
        int.class
    }
)
final class LtEvaluators {
    private LtEvaluators() {
    }

    static boolean lt(int value0, int value1) {
        return value0 < value1;
    }

    static boolean lt(long value0, long value1) {
        return value0 < value1;
    }

    static boolean lt(float value0, float value1) {
        return value0 < value1;
    }

    static boolean lt(double value0, double value1) {
        return value0 < value1;
    }

    static boolean lt(@NonNull BigDecimal value0, BigDecimal value1) {
        return value0.compareTo(value1) < 0;
    }

    static boolean lt(@NonNull String value0, String value1) {
        return value0.compareTo(value1) < 0;
    }

    static boolean lt(@NonNull Date value0, Date value1) {
        return value0.before(value1);
    }

    static boolean lt(@NonNull Time value0, Time value1) {
        return value0.before(value1);
    }

    static boolean lt(@NonNull Timestamp value0, Timestamp value1) {
        return value0.before(value1);
    }
}
