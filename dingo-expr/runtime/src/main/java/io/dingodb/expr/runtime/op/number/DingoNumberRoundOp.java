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

package io.dingodb.expr.runtime.op.number;

import com.google.auto.service.AutoService;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.op.RtFun;
import io.dingodb.expr.runtime.op.RtOp;
import io.dingodb.func.DingoFuncProvider;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class DingoNumberRoundOp extends RtFun {
    public DingoNumberRoundOp(@Nonnull RtExpr[] paras) {
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
        if (divide.abs().compareTo(new BigDecimal(0.1)) < 0) {
            return new BigDecimal(0);
        }

        divide = divide.setScale(0, RoundingMode.HALF_UP);
        return divide.multiply(new BigDecimal(temp));
    }

    public static BigDecimal round(final BigDecimal value) {
        return round(value, 0);
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
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
            int scale = new BigDecimal(String.valueOf(values[1])).setScale(0, BigDecimal.ROUND_HALF_UP)
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

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoNumberRoundOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("round");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoNumberRoundOp.class.getMethod("round", BigDecimal.class));
                methods.add(DingoNumberRoundOp.class.getMethod("round", BigDecimal.class, int.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e, e);
                throw new RuntimeException(e);
            }
        }
    }
}
