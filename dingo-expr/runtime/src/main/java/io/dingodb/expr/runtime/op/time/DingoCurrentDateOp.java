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

import com.google.auto.service.AutoService;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.op.RtFun;
import io.dingodb.expr.runtime.op.RtOp;
import io.dingodb.expr.runtime.op.time.utils.DingoDateTimeUtils;
import io.dingodb.func.DingoFuncProvider;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

@Slf4j
public class DingoCurrentDateOp extends RtFun {

    public DingoCurrentDateOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    public int typeCode() {
        return TypeCode.DATE;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        return getCurrentDate();
    }

    public static java.sql.Date getCurrentDate() {
        LocalDate localDate = LocalDate.now();
        Long millis = localDate.atStartOfDay().toInstant(DingoDateTimeUtils.getLocalZoneOffset()).toEpochMilli();
        return new Date(millis);
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoCurrentDateOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("current_date", "curdate");
        }

        @Override
        public List<Method> methods() {
            List<Method> methods = new ArrayList<>();
            try {
                methods.add(DingoCurrentDateOp.class.getMethod("getCurrentDate"));
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e.toString(), e);
                throw new RuntimeException(e);
            }
            return methods;
        }
    }
}
