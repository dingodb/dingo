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

package io.dingodb.exec.fun;

import io.dingodb.expr.annotations.Operators;
import io.dingodb.expr.runtime.exception.ExprEvaluatingException;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Operators
abstract class PowFun extends BinaryOp {
    public static final String NAME = "POW";

    private static final long serialVersionUID = 6371448137108545795L;

    static double pow(double value0, double value1) {
        double result = Math.pow(value0, value1);
        if (!Double.isNaN(result)) {
            return result;
        }
        throw new ExprEvaluatingException("Evaluating of function " + NAME + " got NaN value for float point type.");
    }

    static @NonNull BigDecimal pow(@NonNull BigDecimal value0, @NonNull BigDecimal value1) {
        if (value1.scale() == 0) {
            try {
                BigDecimal result = value0.pow(value1.intValue());
                if (result.scale() > 0) {
                    result = result.stripTrailingZeros();
                }
                if (result.scale() < 0) {
                    result = result.setScale(0, RoundingMode.HALF_UP);
                }
                return result;
            } catch (ArithmeticException ignored) {
            }
        }
        return BigDecimal.valueOf(pow(value0.doubleValue(), value1.doubleValue()));
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public OpKey keyOf(@NonNull Type type0, @NonNull Type type1) {
        if (Types.DECIMAL.matches(type0) && Types.DECIMAL.matches(type1)
            || Types.DOUBLE.matches(type0) && Types.DOUBLE.matches(type1)) {
            return type0;
        }
        return null;
    }

    @Override
    public OpKey bestKeyOf(@NonNull Type @NonNull [] types) {
        if (Types.INT.matches(types[0]) || Types.LONG.matches(types[0]) || Types.DECIMAL.matches(types[0])) {
            types[0] = Types.DECIMAL;
            types[1] = Types.DECIMAL;
            return types[0];
        } else if (Types.FLOAT.matches(types[0]) || Types.DOUBLE.matches(types[0])) {
            types[0] = Types.DOUBLE;
            types[1] = Types.DOUBLE;
            return types[0];
        }
        return null;
    }
}
