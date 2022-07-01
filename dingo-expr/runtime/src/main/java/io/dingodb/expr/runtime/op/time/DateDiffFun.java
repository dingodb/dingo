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
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import io.dingodb.func.DingoFuncProvider;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

@Slf4j
public class DateDiffFun extends RtFun {
    private static final long serialVersionUID = -8589287644033616224L;

    public DateDiffFun(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    public static long dateDiff(Date value0, Date value1) {
        return DateTimeUtils.dateDiff(value0, value1);
    }

    @Override
    public int typeCode() {
        return TypeCode.INT;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        return dateDiff((Date) values[0], (Date) values[1]);
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {
        @Override
        public List<String> name() {
            return Collections.singletonList("datediff");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DateDiffFun.class.getMethod("dateDiff", Date.class, Date.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e, e);
                throw new RuntimeException(e);
            }
        }
    }
}
