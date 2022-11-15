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
import io.dingodb.expr.core.Casting;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;

@Evaluators
final class IntegerCastEvaluators {
    private IntegerCastEvaluators() {
    }

    static int integerCast(int value) {
        return value;
    }

    static int integerCast(long value) {
        return Casting.longToInt(value);
    }

    static int integerCast(double value) {
        return Casting.doubleToInt(value);
    }

    static int integerCast(boolean value) {
        return value ? 1 : 0;
    }

    static int integerCast(@NonNull BigDecimal value) {
        return Casting.decimalToInt(value);
    }

    static int integerCast(@NonNull String value) {
        return Integer.parseInt(value);
    }
}
