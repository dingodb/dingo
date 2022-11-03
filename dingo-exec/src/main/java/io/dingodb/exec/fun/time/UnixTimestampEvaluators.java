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

package io.dingodb.exec.fun.time;

import io.dingodb.exec.utils.DingoDateTimeUtils;
import io.dingodb.expr.annotations.Evaluators;
import io.dingodb.expr.core.evaluator.Evaluator;
import io.dingodb.expr.core.evaluator.EvaluatorFactory;
import io.dingodb.expr.core.evaluator.EvaluatorKey;
import io.dingodb.expr.core.evaluator.UniversalEvaluator;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Timestamp;

@Evaluators(
    evaluatorKey = EvaluatorKey.class,
    evaluatorBase = Evaluator.class,
    evaluatorFactory = EvaluatorFactory.class,
    universalEvaluator = UniversalEvaluator.class,
    induceSequence = {Long.class, Double.class, BigDecimal.class, Integer.class}
)
public final class UnixTimestampEvaluators {
    public static final String NAME = "unix_timestamp";

    private UnixTimestampEvaluators() {
    }

    static long unixTimestamp(@NonNull Timestamp value) {
        return Math.floorDiv(value.getTime(), 1000L);
    }

    static long unixTimestamp() {
        Timestamp value = DingoDateTimeUtils.currentTimestamp();
        return unixTimestamp(value);
    }

    static long unixTimestamp(long value) {
        return value;
    }
}
