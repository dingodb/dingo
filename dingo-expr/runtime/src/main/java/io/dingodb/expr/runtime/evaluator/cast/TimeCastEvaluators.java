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

package io.dingodb.expr.runtime.evaluator.cast;

import io.dingodb.expr.annotations.Evaluators;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Time;

@Evaluators(
    induceSequence = {Long.class, Integer.class}
)
final class TimeCastEvaluators {
    private TimeCastEvaluators() {
    }

    static @NonNull Time timeCast(long value) {
        return new Time(value);
    }

    static @Nullable Time timeCast(String value) {
        return DateTimeUtils.parseTime(value);
    }

    static @NonNull Time timeCast(@NonNull Time value) {
        return value;
    }
}
