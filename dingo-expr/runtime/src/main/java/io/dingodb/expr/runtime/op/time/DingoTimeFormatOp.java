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
import io.dingodb.expr.runtime.exception.FailParseTime;
import io.dingodb.expr.runtime.op.RtFun;
import io.dingodb.expr.runtime.op.RtOp;
import io.dingodb.expr.runtime.op.time.utils.DingoDateTimeUtils;
import io.dingodb.func.DingoFuncProvider;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Time;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

@Slf4j
public class DingoTimeFormatOp extends RtFun {
    private static Integer TIME_PIVOT = 240000;
    public DingoTimeFormatOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    public int typeCode() {
        return TypeCode.STRING;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        Object value = values[0];
        if ( value instanceof  String && ((String) value).isEmpty()) {
            return null;
        }
        String formatStr = DingoDateTimeUtils.defaultDateFormat();
        if (values.length == 2) {
            formatStr = (String)values[1];
        }

        if (value instanceof Long) {
            return timeFormat((Long) value, formatStr);
        } else if (value instanceof Integer) {
            if ((Integer)value >= TIME_PIVOT) {
                throw new FailParseTime(value.toString() + " can be less than " + TIME_PIVOT);
            }
            // Check value larger than 239999.
            return timeFormat(values[0].toString(), formatStr);
        } else if (value instanceof Time) {
            return timeFormat((Time) values[0], formatStr);
        } else {
            // Check value in hour position greater than 24.
            String time = (String) values[0];
            String hour =  time.contains(":")? time.split(":")[0]: "0";
            if (Integer.valueOf(hour) >= TIME_PIVOT / 10000) {
                throw new FailParseTime("hour " + hour + " can only be less than " + TIME_PIVOT / 10000);
            }
            return timeFormat((String) values[0], formatStr);
        }
    }

    public static String timeFormat(final String time, final String formatStr) {
        if (!time.contains(":")) {
            if (Integer.valueOf(time) >= TIME_PIVOT) {
                throw new FailParseTime(time+ " can be less than " + TIME_PIVOT);
            }
        }
        try {
            LocalTime localTime = DingoDateTimeUtils.convertToTime(time);
            return DingoDateTimeUtils.processFormatStr(localTime, formatStr);
        } catch (SQLException e) {
            throw new FailParseTime(e.getMessage().split("FORMAT")[0], e.getMessage().split("FORMAT")[1]);
        }
    }

    public static String timeFormat(final Long time, final String formatStr) {
        long offsetInMillis = DingoDateTimeUtils.getLocalZoneOffset().getTotalSeconds() * 1000;
        Time t = new Time(time - offsetInMillis);
        return DingoDateTimeUtils.processFormatStr(t.toLocalTime(), formatStr);
    }

    public static String timeFormat(final Time time, final String formatStr) {
        long offsetInMillis = DingoDateTimeUtils.getLocalZoneOffset().getTotalSeconds() * 1000;
        Time t = new Time(time.getTime() - offsetInMillis);
        return DingoDateTimeUtils.processFormatStr(t.toLocalTime(), formatStr);
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoTimeFormatOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("time_format");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoTimeFormatOp.class.getMethod("timeFormat", Long.class, String.class));
                methods.add(DingoTimeFormatOp.class.getMethod("timeFormat", String.class, String.class));
                methods.add(DingoTimeFormatOp.class.getMethod("timeFormat", Time.class, String.class));

                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e.toString(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
