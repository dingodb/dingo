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
import io.dingodb.expr.runtime.evaluator.base.DateEvaluator;
import io.dingodb.expr.runtime.evaluator.base.DecimalEvaluator;
import io.dingodb.expr.runtime.evaluator.base.DoubleEvaluator;
import io.dingodb.expr.runtime.evaluator.base.Evaluator;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorKey;
import io.dingodb.expr.runtime.evaluator.base.IntegerEvaluator;
import io.dingodb.expr.runtime.evaluator.base.LongEvaluator;
import io.dingodb.expr.runtime.evaluator.base.StringEvaluator;
import io.dingodb.expr.runtime.evaluator.base.TimeEvaluator;
import io.dingodb.expr.runtime.evaluator.base.TimestampEvaluator;
import io.dingodb.expr.runtime.evaluator.base.UniversalEvaluator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import javax.annotation.Nonnull;

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

    @Evaluators.Base(IntegerEvaluator.class)
    static int pos(int value) {
        return value;
    }

    @Evaluators.Base(LongEvaluator.class)
    static long pos(long value) {
        return value;
    }

    @Evaluators.Base(DoubleEvaluator.class)
    static double pos(double value) {
        return value;
    }

    @Evaluators.Base(DecimalEvaluator.class)
    static BigDecimal pos(BigDecimal value) {
        return value;
    }

    @Evaluators.Base(IntegerEvaluator.class)
    static int neg(int value) {
        return -value;
    }

    @Evaluators.Base(LongEvaluator.class)
    static long neg(long value) {
        return -value;
    }

    @Evaluators.Base(DoubleEvaluator.class)
    static double neg(double value) {
        return -value;
    }

    @Nonnull
    @Evaluators.Base(DecimalEvaluator.class)
    static BigDecimal neg(@Nonnull BigDecimal value) {
        return value.negate();
    }

    //
    // Binary operators
    //

    @Evaluators.Base(IntegerEvaluator.class)
    static int add(int value0, int value1) {
        return value0 + value1;
    }

    @Evaluators.Base(LongEvaluator.class)
    static long add(long value0, long value1) {
        return value0 + value1;
    }

    @Evaluators.Base(DoubleEvaluator.class)
    static double add(double value0, double value1) {
        return value0 + value1;
    }

    @Nonnull
    @Evaluators.Base(DecimalEvaluator.class)
    static BigDecimal add(@Nonnull BigDecimal value0, BigDecimal value1) {
        return value0.add(value1);
    }

    // This is not arithmetic op, but put here to share the same evaluator factory.
    @Nonnull
    static String add(String s0, String s1) {
        return s0 + s1;
    }

    @Evaluators.Base(IntegerEvaluator.class)
    static int sub(int value0, int value1) {
        return value0 - value1;
    }

    @Evaluators.Base(LongEvaluator.class)
    static long sub(long value0, long value1) {
        return value0 - value1;
    }

    @Evaluators.Base(DoubleEvaluator.class)
    static double sub(double value0, double value1) {
        return value0 - value1;
    }

    @Nonnull
    @Evaluators.Base(DecimalEvaluator.class)
    static BigDecimal sub(@Nonnull BigDecimal value0, BigDecimal value1) {
        return value0.subtract(value1);
    }

    @Evaluators.Base(IntegerEvaluator.class)
    static int mul(int value0, int value1) {
        return value0 * value1;
    }

    @Evaluators.Base(LongEvaluator.class)
    static long mul(long value0, long value1) {
        return value0 * value1;
    }

    @Evaluators.Base(DoubleEvaluator.class)
    static double mul(double value0, double value1) {
        return value0 * value1;
    }

    @Nonnull
    @Evaluators.Base(DecimalEvaluator.class)
    static BigDecimal mul(@Nonnull BigDecimal value0, BigDecimal value1) {
        return value0.multiply(value1);
    }

    @Evaluators.Base(IntegerEvaluator.class)
    static int div(int value0, int value1) {
        return value0 / value1;
    }

    @Evaluators.Base(LongEvaluator.class)
    static long div(long value0, long value1) {
        return value0 / value1;
    }

    @Evaluators.Base(DoubleEvaluator.class)
    static double div(double value0, double value1) {
        return value0 / value1;
    }

    @Nonnull
    @Evaluators.Base(DecimalEvaluator.class)
    static BigDecimal div(@Nonnull BigDecimal value0, BigDecimal value1) {
        return value0.divide(value1, RoundingMode.HALF_EVEN);
    }

    @Evaluators.Base(IntegerEvaluator.class)
    static int abs(int num) {
        return Math.abs(num);
    }

    @Evaluators.Base(LongEvaluator.class)
    static long abs(long num) {
        return Math.abs(num);
    }

    @Evaluators.Base(DoubleEvaluator.class)
    static double abs(double num) {
        return Math.abs(num);
    }

    @Evaluators.Base(IntegerEvaluator.class)
    static int min(int value0, int value1) {
        return Math.min(value0, value1);
    }

    @Evaluators.Base(LongEvaluator.class)
    static long min(long value0, long value1) {
        return Math.min(value0, value1);
    }

    @Evaluators.Base(DoubleEvaluator.class)
    static double min(double value0, double value1) {
        return Math.min(value0, value1);
    }

    @Evaluators.Base(DecimalEvaluator.class)
    static BigDecimal min(@Nonnull BigDecimal value0, BigDecimal value1) {
        return value0.compareTo(value1) <= 0 ? value0 : value1;
    }

    @Evaluators.Base(IntegerEvaluator.class)
    static int max(int value0, int value1) {
        return Math.max(value0, value1);
    }

    @Evaluators.Base(LongEvaluator.class)
    static long max(long value0, long value1) {
        return Math.max(value0, value1);
    }

    @Evaluators.Base(DoubleEvaluator.class)
    static double max(double value0, double value1) {
        return Math.max(value0, value1);
    }

    @Evaluators.Base(DecimalEvaluator.class)
    static BigDecimal max(@Nonnull BigDecimal value0, BigDecimal value1) {
        return value0.compareTo(value1) >= 0 ? value0 : value1;
    }

    /**
     * These are string op, put here to share the same evaluator factory.
     */

    @Evaluators.Base(StringEvaluator.class)
    static String min(@Nonnull String value0, String value1) {
        return value0.compareTo(value1) <= 0 ? value0 : value1;
    }

    @Evaluators.Base(StringEvaluator.class)
    static String max(@Nonnull String value0, String value1) {
        return value0.compareTo(value1) >= 0 ? value0 : value1;
    }

    /**
     * These are date time ops put here to share the same evaluator factory.
     */

    @Evaluators.Base(DateEvaluator.class)
    static Date min(@Nonnull Date value0, Date value1) {
        return value0.compareTo(value1) < 0 ? value0 : value1;
    }

    @Evaluators.Base(DateEvaluator.class)
    static Date max(@Nonnull Date value0, Date value1) {
        return value0.compareTo(value1) >= 0 ? value0 : value1;
    }

    @Evaluators.Base(TimeEvaluator.class)
    static Time min(@Nonnull Time value0, Time value1) {
        return value0.compareTo(value1) < 0 ? value0 : value1;
    }

    @Evaluators.Base(TimeEvaluator.class)
    static Time max(@Nonnull Time value0, Time value1) {
        return value0.compareTo(value1) >= 0 ? value0 : value1;
    }

    @Evaluators.Base(TimestampEvaluator.class)
    static Timestamp min(@Nonnull Timestamp value0, Timestamp value1) {
        return value0.compareTo(value1) < 0 ? value0 : value1;
    }

    @Evaluators.Base(TimestampEvaluator.class)
    static Timestamp max(@Nonnull Timestamp value0, Timestamp value1) {
        return value0.compareTo(value1) >= 0 ? value0 : value1;
    }
}
