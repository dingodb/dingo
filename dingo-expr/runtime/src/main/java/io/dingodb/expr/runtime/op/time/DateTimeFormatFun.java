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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

@Slf4j
public class DateTimeFormatFun extends RtFun {
    private static final long serialVersionUID = -4046571111287193650L;

    public DateTimeFormatFun(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Nonnull
    public static String dateTimeFormat(@Nonnull Timestamp value, @Nonnull String format) {
        return DateTimeUtils.dateTimeFormat(value, format);
    }

    @Nonnull
    public static String dateTimeFormat(@Nonnull Timestamp value) {
        return DateTimeUtils.dateTimeFormat(value);
    }

    @Override
    public int typeCode() {
        return TypeCode.STRING;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        Timestamp value = (Timestamp) values[0];
        if (values.length < 2) {
            return dateTimeFormat(value);
        }
        String format = (String) values[1];
        return dateTimeFormat(value, format);
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {
        @Override
        public List<String> name() {
            return Collections.singletonList("datetime_format");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DateTimeFormatFun.class.getMethod("dateTimeFormat", Timestamp.class, String.class));
                methods.add(DateTimeFormatFun.class.getMethod("dateTimeFormat", Timestamp.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e, e);
                throw new RuntimeException(e);
            }
        }
    }
}
