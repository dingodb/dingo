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
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;

@Evaluators(
    induceSequence = {
        BigDecimal.class,
        double.class,
        float.class,
        long.class,
        int.class
    }
)
public final class ModEvaluators {
    private ModEvaluators() {
    }

    static int mod(int value0, int value1) {
        return value0 % value1;
    }

    static long mod(long value0, long value1) {
        return value0 % value1;
    }

    static @NonNull BigDecimal mod(@NonNull BigDecimal value0, @NonNull BigDecimal value1) {
        return value0.remainder(value1);
    }
}
