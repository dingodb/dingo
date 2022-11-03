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
import io.dingodb.expr.runtime.utils.NumberUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Evaluators
final class IntegerCastEvaluators {
    private IntegerCastEvaluators() {
    }

    static int integerCast(int value) {
        return value;
    }

    static int integerCast(long value) {
        // TODO: is it appropriate to check in runtime?
        return NumberUtils.checkIntRange(value);
    }

    static int integerCast(double value) {
        return (int) Math.round(value);
    }

    static int integerCast(boolean value) {
        return value ? 1 : 0;
    }

    static int integerCast(@NonNull BigDecimal value) {
        return value.setScale(0, RoundingMode.HALF_UP).intValue();
    }

    static int integerCast(@NonNull String value) {
        return Integer.parseInt(value);
    }
}
