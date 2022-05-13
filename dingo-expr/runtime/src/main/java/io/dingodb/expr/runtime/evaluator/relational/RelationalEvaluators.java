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
import io.dingodb.expr.runtime.evaluator.base.BooleanEvaluator;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorKey;
import io.dingodb.expr.runtime.evaluator.base.UniversalEvaluator;

import java.math.BigDecimal;
import java.sql.Date;
import javax.annotation.Nonnull;

@Evaluators(
    evaluatorKey = EvaluatorKey.class,
    evaluatorBase = BooleanEvaluator.class,
    evaluatorFactory = EvaluatorFactory.class,
    universalEvaluator = UniversalEvaluator.class,
    induceSequence = {BigDecimal.class, Double.class, Long.class, Integer.class}
)
final class RelationalEvaluators {
    private RelationalEvaluators() {
    }

    static boolean eq(boolean first, boolean second) {
        return first == second;
    }

    static boolean eq(int first, int second) {
        return first == second;
    }

    static boolean eq(long first, long second) {
        return first == second;
    }

    static boolean eq(double first, double second) {
        return first == second;
    }

    static boolean eq(@Nonnull BigDecimal first, BigDecimal second) {
        return first.compareTo(second) == 0;
    }

    static boolean eq(@Nonnull String first, String second) {
        return first.equals(second);
    }

    static boolean eq(@Nonnull Date first, Date second) {
        return first.equals(second);
    }

    static boolean eq1(@Nonnull Object first, Object second) {
        return first.equals(second);
    }

    static boolean lt(int first, int second) {
        return first < second;
    }

    static boolean lt(long first, long second) {
        return first < second;
    }

    static boolean lt(double first, double second) {
        return first < second;
    }

    static boolean lt(@Nonnull BigDecimal first, BigDecimal second) {
        return first.compareTo(second) < 0;
    }

    static boolean lt(@Nonnull String first, String second) {
        return first.compareTo(second) < 0;
    }

    static boolean lt(@Nonnull Date first, Date second) {
        return first.before(second);
    }

    static boolean le(int first, int second) {
        return first <= second;
    }

    static boolean le(long first, long second) {
        return first <= second;
    }

    static boolean le(double first, double second) {
        return first <= second;
    }

    static boolean le(@Nonnull Date first, Date second) {
        return !first.after(second);
    }

    static boolean le(@Nonnull BigDecimal first, BigDecimal second) {
        return first.compareTo(second) <= 0;
    }

    static boolean le(@Nonnull String first, String second) {
        return first.compareTo(second) <= 0;
    }

    static boolean gt(int first, int second) {
        return first > second;
    }

    static boolean gt(long first, long second) {
        return first > second;
    }

    static boolean gt(double first, double second) {
        return first > second;
    }

    static boolean gt(@Nonnull BigDecimal first, BigDecimal second) {
        return first.compareTo(second) > 0;
    }

    static boolean gt(@Nonnull String first, String second) {
        return first.compareTo(second) > 0;
    }

    static boolean gt(@Nonnull Date first, Date second) {
        return first.after(second);
    }

    static boolean ge(int first, int second) {
        return first >= second;
    }

    static boolean ge(long first, long second) {
        return first >= second;
    }

    static boolean ge(double first, double second) {
        return first >= second;
    }

    static boolean ge(@Nonnull BigDecimal first, BigDecimal second) {
        return first.compareTo(second) >= 0;
    }

    static boolean ge(@Nonnull String first, String second) {
        return first.compareTo(second) >= 0;
    }

    static boolean ge(@Nonnull Date first, Date second) {
        return !first.before(second);
    }

    static boolean ne(boolean first, boolean second) {
        return first != second;
    }

    static boolean ne(int first, int second) {
        return first != second;
    }

    static boolean ne(long first, long second) {
        return first != second;
    }

    static boolean ne(double first, double second) {
        return first != second;
    }

    static boolean ne(@Nonnull BigDecimal first, BigDecimal second) {
        return first.compareTo(second) != 0;
    }

    static boolean ne(@Nonnull String first, String second) {
        return first.compareTo(second) != 0;
    }

    static boolean ne(@Nonnull Date first, Date second) {
        return !first.equals(second);
    }
}
