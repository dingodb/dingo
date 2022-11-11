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

import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtFun;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Slf4j
public class PowFun extends RtFun {
    public static final String NAME = "pow";
    private static final long serialVersionUID = 2219616474020952033L;

    public PowFun(RtExpr[] paras) {
        super(paras);
    }

    public static BigDecimal pow(final BigDecimal value, final @NonNull BigDecimal power) {
        int powerInt = power.intValue();
        if (powerInt < 0) {
            try {
                return BigDecimal.ONE.divide(value.pow(-powerInt));
            } catch (ArithmeticException exception) {
                return BigDecimal.ONE.divide(value.pow(-powerInt), 16, RoundingMode.HALF_DOWN);
            }

        }

        BigDecimal intDecimal = value.setScale(0, RoundingMode.FLOOR);
        if (value.compareTo(intDecimal) == 0) {
            return intDecimal.pow(powerInt);
        }
        return value.pow(powerInt);
    }

    @Override
    protected @Nullable Object fun(Object @NonNull [] values) {
        if (values[0] == null || values[1] == null) {
            return null;
        }

        BigDecimal value = new BigDecimal(String.valueOf(values[0]));
        BigDecimal power = new BigDecimal(String.valueOf(values[1]));
        if (power.scale() > 0) {
            throw new RuntimeException("Parameter is invalid");
        }
        return pow(value, power);
    }

    @Override
    public int typeCode() {
        return TypeCode.DECIMAL;
    }
}
