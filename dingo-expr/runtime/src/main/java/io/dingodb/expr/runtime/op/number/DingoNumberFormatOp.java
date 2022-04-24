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

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

@Slf4j
public class DingoNumberFormatOp extends RtFun  {
    private static final long serialVersionUID = 4805636716328583550L;

    public DingoNumberFormatOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        int inputScalar = new BigDecimal(String.valueOf(values[1]))
            .setScale(0, BigDecimal.ROUND_HALF_UP).intValue();
        if (inputScalar < 0) {
            inputScalar = 0;
        }

        BigDecimal decimal = new BigDecimal(String.valueOf(values[0])).setScale(inputScalar, BigDecimal.ROUND_HALF_UP);
        return formatNumber(decimal.doubleValue(), inputScalar);
    }

    @Override
    public int typeCode() {
        return TypeCode.DOUBLE;
    }

    public static String formatNumber(final double value, int scale) {
        if (scale < 0) {
            scale = 0;
        }

        BigDecimal decimal = new BigDecimal(value).setScale(scale, BigDecimal.ROUND_HALF_UP);
        return decimal.toString();
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoNumberFormatOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("format");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoNumberFormatOp.class.getMethod("formatNumber", double.class, int.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e.toString(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
