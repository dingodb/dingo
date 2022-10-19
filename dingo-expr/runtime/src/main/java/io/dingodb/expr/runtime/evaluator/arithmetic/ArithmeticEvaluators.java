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

package io.dingodb.expr.runtime.evaluator.arithmetic;

import io.dingodb.expr.annotations.Evaluators;
import io.dingodb.expr.runtime.evaluator.base.Evaluator;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorKey;
import io.dingodb.expr.runtime.evaluator.base.UniversalEvaluator;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Slf4j
@Evaluators(
    evaluatorKey = EvaluatorKey.class,
    evaluatorBase = Evaluator.class,
    evaluatorFactory = EvaluatorFactory.class,
    universalEvaluator = UniversalEvaluator.class,
    induceSequence = {BigDecimal.class, Double.class, Long.class, Integer.class}
)
final class ArithmeticEvaluators {
    private ArithmeticEvaluators() {
    }

    //
    // Unary operators
    //

    static int pos(int value) {
        return value;
    }

    static long pos(long value) {
        return value;
    }

    static double pos(double value) {
        return value;
    }

    static BigDecimal pos(BigDecimal value) {
        return value;
    }

    static int neg(int value) {
        return -value;
    }

    static long neg(long value) {
        return -value;
    }

    static double neg(double value) {
        return -value;
    }

    static @NonNull BigDecimal neg(@NonNull BigDecimal value) {
        return value.negate();
    }

    //
    // Binary operators
    //

    static int add(int value0, int value1) {
        return value0 + value1;
    }

    static long add(long value0, long value1) {
        return value0 + value1;
    }

    static double add(double value0, double value1) {
        return value0 + value1;
    }

    static @NonNull BigDecimal add(@NonNull BigDecimal value0, BigDecimal value1) {
        return value0.add(value1);
    }

    // This is not arithmetic op, but put here to share the same evaluator factory.
    static @NonNull String add(String s0, String s1) {
        return s0 + s1;
    }

    static int sub(int value0, int value1) {
        return value0 - value1;
    }

    static long sub(long value0, long value1) {
        return value0 - value1;
    }

    static double sub(double value0, double value1) {
        return value0 - value1;
    }

    static @NonNull BigDecimal sub(@NonNull BigDecimal value0, BigDecimal value1) {
        return value0.subtract(value1);
    }

    static int mul(int value0, int value1) {
        return value0 * value1;
    }

    static long mul(long value0, long value1) {
        return value0 * value1;
    }

    static double mul(double value0, double value1) {
        return value0 * value1;
    }

    static @NonNull BigDecimal mul(@NonNull BigDecimal value0, BigDecimal value1) {
        return value0.multiply(value1);
    }

    static int div(int value0, int value1) {
        return value0 / value1;
    }

    static long div(long value0, long value1) {
        return value0 / value1;
    }

    static double div(double value0, double value1) {
        return value0 / value1;
    }

    static @NonNull BigDecimal div(@NonNull BigDecimal value0, BigDecimal value1) {
        return value0.divide(value1, RoundingMode.HALF_EVEN);
    }

    static int abs(int num) {
        return Math.abs(num);
    }

    static long abs(long num) {
        return Math.abs(num);
    }

    static double abs(double num) {
        return Math.abs(num);
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

    static BigDecimal min(@NonNull BigDecimal value0, BigDecimal value1) {
        return value0.compareTo(value1) <= 0 ? value0 : value1;
    }

    // This is not arithmetic op, but put here to share the same evaluator factory.
    static String min(@NonNull String value0, String value1) {
        return value0.compareTo(value1) <= 0 ? value0 : value1;
    }

    // This is not arithmetic op, but put here to share the same evaluator factory.
    static Date min(@NonNull Date value0, Date value1) {
        return value0.compareTo(value1) <= 0 ? value0 : value1;
    }

    // This is not arithmetic op, but put here to share the same evaluator factory.
    static Time min(@NonNull Time value0, Time value1) {
        return value0.compareTo(value1) <= 0 ? value0 : value1;
    }

    // This is not arithmetic op, but put here to share the same evaluator factory.
    static Timestamp min(@NonNull Timestamp value0, Timestamp value1) {
        return value0.compareTo(value1) < 0 ? value0 : value1;
    }

    static int max(int value0, int value1) {
        return Math.max(value0, value1);
    }

    static long max(long value0, long value1) {
        return Math.max(value0, value1);
    }

    static double max(double value0, double value1) {
        return Math.max(value0, value1);
    }

    static BigDecimal max(@NonNull BigDecimal value0, BigDecimal value1) {
        return value0.compareTo(value1) >= 0 ? value0 : value1;
    }

    // This is not arithmetic op, but put here to share the same evaluator factory.
    static String max(@NonNull String value0, String value1) {
        return value0.compareTo(value1) >= 0 ? value0 : value1;
    }

    // This is not arithmetic op, but put here to share the same evaluator factory.
    static Date max(@NonNull Date value0, Date value1) {
        return value0.compareTo(value1) >= 0 ? value0 : value1;
    }

    // This is not arithmetic op, but put here to share the same evaluator factory.
    static Time max(@NonNull Time value0, Time value1) {
        return value0.compareTo(value1) >= 0 ? value0 : value1;
    }

    // This is not arithmetic op, but put here to share the same evaluator factory.
    static Timestamp max(@NonNull Timestamp value0, Timestamp value1) {
        return value0.compareTo(value1) >= 0 ? value0 : value1;
    }
}
