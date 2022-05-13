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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

@Slf4j
public class DingoDateFromUnixTimeOp extends RtFun {
    public DingoDateFromUnixTimeOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    public int typeCode() {
        return TypeCode.TIMESTAMP;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        String timeStr = String.valueOf(values[0]);
        if (timeStr == null) {
            throw new IllegalArgumentException("incorrect value of unix_timestamp for from_unixtime function");
        }

        Long unixTime = 0L;
        if (timeStr.length() < 13) {
            unixTime = Long.valueOf(timeStr) * 1000;
        } else if (timeStr.length() == 13) {
            unixTime = Long.valueOf(timeStr);
        }

        if (values.length == 2) {
            String formatStr = String.valueOf(values[1]);
            LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(unixTime), ZoneId.systemDefault());
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(formatStr);
            return formatter.format(dateTime);
        } else {
            return fromUnixTime(unixTime);
        }
    }

    public static String fromUnixTime(final Long timestamp) {
        DateTimeFormatter formatter = DingoDateTimeUtils.getDatetimeFormatter();
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        return formatter.format(dateTime);
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoDateFromUnixTimeOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("from_unixtime");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoDateFromUnixTimeOp.class.getMethod("fromUnixTime", Long.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e.toString(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
