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
public class DingoNumberCeilingOp extends RtFun {
    public DingoNumberCeilingOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    public static BigDecimal ceiling(final BigDecimal value) {
        return value.setScale(0, RoundingMode.CEILING);
    }

    @org.apache.calcite.linq4j.function.Hints("SqlKind:CEIL")
    public static BigDecimal ceil(final BigDecimal value) {
        return ceiling(value);
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        if (values[0] == null) {
            return null;
        }

        BigDecimal value = new BigDecimal(String.valueOf(values[0]));
        return ceiling(value);
    }

    @Override
    public int typeCode() {
        return TypeCode.DECIMAL;
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoNumberCeilingOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("ceiling", "ceil");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoNumberCeilingOp.class.getMethod("ceiling", BigDecimal.class));
                methods.add(DingoNumberCeilingOp.class.getMethod("ceil", BigDecimal.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e, e);
                throw new RuntimeException(e);
            }
        }
    }
}
