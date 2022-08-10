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
import io.dingodb.expr.runtime.evaluator.base.Evaluator;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorKey;
import io.dingodb.expr.runtime.evaluator.base.UniversalEvaluator;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import javax.annotation.Nonnull;

@Evaluators(
    evaluatorKey = EvaluatorKey.class,
    evaluatorBase = Evaluator.class,
    evaluatorFactory = EvaluatorFactory.class,
    universalEvaluator = UniversalEvaluator.class,
    induceSequence = {BigDecimal.class, Double.class, Long.class, Integer.class}
)
final class NeEvaluators {
    private NeEvaluators() {
    }

    static boolean ne(int value0, int value1) {
        return value0 != value1;
    }

    static boolean ne(long value0, long value1) {
        return value0 != value1;
    }

    static boolean ne(double value0, double value1) {
        return value0 != value1;
    }

    static boolean ne(boolean value0, boolean value1) {
        return value0 != value1;
    }

    static boolean ne(byte[] value0, byte[] value1) {
        return !Arrays.equals(value0, value1);
    }

    static boolean ne(@Nonnull BigDecimal value0, BigDecimal value1) {
        return value0.compareTo(value1) != 0;
    }

    static boolean ne(@Nonnull String value0, String value1) {
        return value0.compareTo(value1) != 0;
    }

    static boolean ne(@Nonnull Date value0, Date value1) {
        return !value0.equals(value1);
    }

    static boolean ne(@Nonnull Time value0, Time value1) {
        return !value0.equals(value1);
    }

    static boolean ne(@Nonnull Timestamp value0, Timestamp value1) {
        return !value0.equals(value1);
    }
}
