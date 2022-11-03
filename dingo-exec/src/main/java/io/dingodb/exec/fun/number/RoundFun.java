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
public class RoundFun extends RtFun {
    public static final String NAME = "round";
    private static final long serialVersionUID = 1728871160707759814L;

    public RoundFun(RtExpr[] paras) {
        super(paras);
    }

    public static BigDecimal round(final BigDecimal value, final int scale) {
        if (scale >= 0) {
            return value.setScale(scale, RoundingMode.HALF_UP);
        }

        long temp = 1;
        for (int i = 1; i <= (-scale); i++) {
            temp = temp * 10;
        }

        BigDecimal divide = value.divide(new BigDecimal(temp));
        if (divide.abs().compareTo(new BigDecimal("0.1")) < 0) {
            return new BigDecimal(0);
        }

        divide = divide.setScale(0, RoundingMode.HALF_UP);
        return divide.multiply(new BigDecimal(temp));
    }

    public static BigDecimal round(final BigDecimal value) {
        return round(value, 0);
    }

    @Override
    protected @Nullable Object fun(Object @NonNull [] values) {
        if (values.length == 1) {
            if (values[0] == null) {
                return null;
            }

            BigDecimal value = new BigDecimal(String.valueOf(values[0]));
            return round(value);
        } else if (values.length == 2) {
            if (values[0] == null || values[1] == null) {
                return null;
            }

            BigDecimal value = new BigDecimal(String.valueOf(values[0]));
            int scale = new BigDecimal(String.valueOf(values[1])).setScale(0, RoundingMode.HALF_UP)
                .intValue();
            if (scale > 10 || scale < -10) {
                throw new RuntimeException("Parameter out of range");
            }

            // If value is integer and scale > 0, return the original value
            if (scale > 0 && value.scale() == 0) {
                return value;
            }

            return round(value, scale);
        }
        return null;
    }

    @Override
    public int typeCode() {
        return TypeCode.DECIMAL;
    }
}
