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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Evaluators(
    induceSequence = {
        String.class,
        BigDecimal.class,
        double.class,
        float.class,
        long.class,
        int.class,
        boolean.class,
        byte[].class
    }
)
final class StringCastEvaluators {
    private StringCastEvaluators() {
    }

    static @NonNull String stringCast(@NonNull String value) {
        return value;
    }

    static @NonNull String stringCast(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    static @NonNull String stringCast(Date value) {
        return DateTimeUtils.dateFormat(value);
    }

    static @NonNull String stringCast(Time value) {
        return DateTimeUtils.timeFormat(value);
    }

    static @NonNull String stringCast(Timestamp value) {
        return DateTimeUtils.timestampFormat(value);
    }
}
