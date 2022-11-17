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

package io.dingodb.exec.fun.number;

import io.dingodb.expr.annotations.Evaluators;
import io.dingodb.expr.core.Casting;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;

@Evaluators
final class AbsEvaluators {
    private AbsEvaluators() {
    }

    static int abs(int num) {
        if (num == Integer.MIN_VALUE) {
            throw Casting.intRangeException("abs(" + num + ")");
        }
        return Math.abs(num);
    }

    static long abs(long num) {
        if (num == Long.MIN_VALUE) {
            throw Casting.longRangeException("abs(" + num + ")");
        }
        return Math.abs(num);
    }

    static double abs(double num) {
        return Math.abs(num);
    }

    static @NonNull BigDecimal abs(@NonNull BigDecimal num) {
        return num.abs();
    }
}
