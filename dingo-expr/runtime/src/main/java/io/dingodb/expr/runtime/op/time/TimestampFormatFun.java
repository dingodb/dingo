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
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class TimestampFormatFun extends RtFun {
    private static final long serialVersionUID = -4046571111287193650L;

    public TimestampFormatFun(RtExpr[] paras) {
        super(paras);
    }

    public static @NonNull String timestampFormat(Timestamp value, String format) {
        return DateTimeUtils.timestampFormat(value, format);
    }

    public static @NonNull String timestampFormat(Timestamp value) {
        return DateTimeUtils.timestampFormat(value);
    }

    @Override
    public int typeCode() {
        return TypeCode.STRING;
    }

    @Override
    protected Object fun(Object @NonNull [] values) {
        Timestamp value = (Timestamp) values[0];
        if (values.length < 2) {
            return timestampFormat(value);
        }
        String format = (String) values[1];
        return timestampFormat(value, format);
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {
        @Override
        public List<String> name() {
            return Collections.singletonList("timestamp_format");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(TimestampFormatFun.class.getMethod("timestampFormat", Timestamp.class, String.class));
                methods.add(TimestampFormatFun.class.getMethod("timestampFormat", Timestamp.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e, e);
                throw new RuntimeException(e);
            }
        }
    }
}
