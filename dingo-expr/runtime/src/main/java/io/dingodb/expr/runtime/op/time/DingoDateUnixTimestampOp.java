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
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

@Slf4j
public class DingoDateUnixTimestampOp extends RtFun {

    public DingoDateUnixTimestampOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    static final ZoneOffset ZONE_OFFSET = DingoDateTimeUtils.getLocalZoneOffset();

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

        if (value instanceof BigInteger) {
            return unixTimestamp(((BigInteger) value).longValue());
        }

        if (value instanceof Long) {
            return unixTimestamp((Long) value);
        }

        if (value instanceof Integer) {
            return unixTimestamp(((Integer) value).longValue());
        }
        // If value is String(such as unix_timestamp('2022-04-28'), then convert to long of zone.
        if (value instanceof String) {
            return unixTimestamp((String) value) / 1000;
        }

        // If value is Date(such as unix_timestamp(current_date)), then convert to long of zone.
        if (value instanceof Date) {
            return unixTimestamp((Date) value) / 1000;
        }

        throw new IllegalArgumentException();
    }

    public static Long unixTimestamp(final String input) {
        int length = input.length();
        // Does not include hours, minutes and seconds
        if (length < 14) {
            try {
                LocalDate date = DingoDateTimeUtils.convertToDate(input);
                return date.atStartOfDay().toInstant(ZONE_OFFSET).toEpochMilli();
            } catch (SQLException e) {
                throw new FailParseTime(e.getMessage().split("FORMAT")[0], e.getMessage().split("FORMAT")[1]);
            }
        } else {
            // Include hours, minutes and seconds
            try {
                LocalDateTime dateTime = DingoDateTimeUtils.convertToDatetime(input);
                Long ts = dateTime.toInstant(ZONE_OFFSET).toEpochMilli();
                return ts;
            } catch (SQLException e) {
                log.error(e.getMessage());
                throw new FailParseTime(e.getMessage(), "");
            }
        }
    }

    public static Long unixTimestamp(final Long input) {
        return input;
    }

    public static Long unixTimestamp(final Date input) {
        Long epochMilli = input.getTime();
        LocalDate localDate = new Date(epochMilli).toLocalDate();
        return localDate.atStartOfDay(ZONE_OFFSET).toInstant().toEpochMilli();
    }

    public static Long unixTimestamp() {
        return LocalDateTime.now().toInstant(ZONE_OFFSET).toEpochMilli();
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoDateUnixTimestampOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("unix_timestamp");
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
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e.toString(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
