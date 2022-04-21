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
import io.dingodb.func.DingoFuncProvider;

import java.lang.reflect.Method;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;


public class DingoDateUnixTimestampOp extends RtFun {

    public DingoDateUnixTimestampOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    static final List<String> FORMAT_LIST = Stream.of(
        "yyyyMMdd",
        "yyyy-M-d",
        "yyyy/M/d",
        "yyyyMMddHHmmss",
        "yyyy-M-d HH:mm:ss",
        "yyyy/M/d HH:mm:ss",
        "yyyy.M.d",
        "yyyy.M.d HH:mm:ss"
    ).collect(Collectors.toList());

    static final ZoneOffset ZONEOFFSET = ZoneOffset.ofHours(8);

    @Override
    public int typeCode() {
        return TypeCode.LONG;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        if (values.length < 1) {
            return unixTimestamp() / 1000;
        }

        Object value = values[0];
        if (value instanceof Long) {
            return unixTimestamp((Long) value);
        }
        if (value instanceof String) {
            return unixTimestamp((String) value) / 1000;
        }

        throw new IllegalArgumentException();
    }

    public static Long unixTimestamp(final String input) {
        int length = input.length();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("");
        // Does not include hours, minutes and seconds
        if (length < 14) {
            if (input.contains("-")) {
                formatter = DateTimeFormatter.ofPattern(FORMAT_LIST.get(1));
            } else if (input.contains("/")) {
                formatter = DateTimeFormatter.ofPattern(FORMAT_LIST.get(2));
            } else if (input.contains(".")) {
                formatter = DateTimeFormatter.ofPattern(FORMAT_LIST.get(6));
            } else {
                formatter = DateTimeFormatter.ofPattern(FORMAT_LIST.get(0));
            }

            LocalDate date = LocalDate.parse(input, formatter);
            return date.atStartOfDay().toInstant(ZONEOFFSET).toEpochMilli();
        } else {
            // Include hours, minutes and seconds
            if (input.contains("-")) {
                formatter = DateTimeFormatter.ofPattern(FORMAT_LIST.get(4));
            } else if (input.contains("/")) {
                formatter = DateTimeFormatter.ofPattern(FORMAT_LIST.get(5));
            } else if (input.contains(".")) {
                formatter = DateTimeFormatter.ofPattern(FORMAT_LIST.get(7));
            } else {
                formatter = DateTimeFormatter.ofPattern(FORMAT_LIST.get(3));
            }

            LocalDateTime dateTime = LocalDateTime.parse(input, formatter);
            return dateTime.toInstant(ZONEOFFSET).toEpochMilli();
        }
    }

    public static Long unixTimestamp(final Long input) {
        return input;
    }

    public static Long unixTimestamp(final Date input) {
        return input.getTime();
    }

    public static Long unixTimestamp() {
        return LocalDateTime.now().toInstant(ZONEOFFSET).toEpochMilli();
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoDateUnixTimestampOp::new;
        }

        @Override
        public String name() {
            return "unix_timestamp";
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoDateUnixTimestampOp.class.getMethod("unixTimestamp"));
                methods.add(DingoDateUnixTimestampOp.class.getMethod("unixTimestamp", String.class));
                methods.add(DingoDateUnixTimestampOp.class.getMethod("unixTimestamp", Long.class));
                methods.add(DingoDateUnixTimestampOp.class.getMethod("unixTimestamp", Date.class));
                return methods;
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
    }


}
