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

package io.dingodb.expr.runtime.op.time;

import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.op.RtFun;
import io.dingodb.expr.runtime.op.RtOp;
import io.dingodb.expr.runtime.op.time.utils.DateFormatUtil;
import io.dingodb.func.DingoFuncProvider;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Create an DingoDteNowOp to process date.
 */

@Slf4j
public class DingoDateNowOp extends RtFun {

    public static final long serialVersionUID = -1436327540637259764L;

    public DingoDateNowOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    public int typeCode() {
        return TypeCode.STRING;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        return getLocalTime();
    }

    public static String getLocalTime() {
        String formatStr = DateFormatUtil.defaultDatetimeFormat();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(formatStr);
        return new java.sql.Timestamp(System.currentTimeMillis()).toLocalDateTime().format(formatter);
    }

    /**
     * the now function is equal to current_timestamp.
     * then disable now function.
     */
    // @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {
        public Function<RtExpr[], RtOp> supplier() {
            return DingoDateNowOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("now");
        }

        @Override
        public List<Method> methods() {
            List<Method> methods = new ArrayList<>();
            try {
                methods.add(DingoDateNowOp.class.getMethod("getLocalTime"));
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e.toString(), e);
                throw new RuntimeException(e);
            }
            return methods;
        }
    }

}
