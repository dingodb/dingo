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

package io.dingodb.exec.fun.mysql;

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.TertiaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

/**
 * MySQL CONVERT_TZ() function.
 *
 * <p>Converts a datetime value from one time zone to another. The time zone
 * arguments accept IANA zone ids (e.g. 'UTC', 'Asia/Shanghai', 'GMT+8'),
 * numeric offsets ('+08:00', '-05:30') and 'SYSTEM' for the server default.
 * Invalid time zone arguments yield SQL NULL, following MySQL semantics.</p>
 */
public class ConvertTzFun extends TertiaryOp {
    public static final String NAME = "convert_tz";
    @SuppressWarnings("serial")
    private static final long serialVersionUID = -5290214786024869011L;

    public static final ConvertTzFun INSTANCE = new ConvertTzFun();

    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .appendPattern("yyyy-MM-dd HH:mm:ss")
        .optionalStart()
        .appendFraction(ChronoField.NANO_OF_SECOND, 1, 6, true)
        .optionalEnd()
        .toFormatter();

    @Override
    protected Object evalNonNullValue(
        @NonNull Object value0, @NonNull Object value1, @NonNull Object value2, ExprConfig config
    ) {
        ZoneId fromZone = parseTimeZone(value1.toString());
        ZoneId toZone = parseTimeZone(value2.toString());
        if (fromZone == null || toZone == null) {
            return null;
        }
        LocalDateTime localDateTime = toLocalDateTime(value0);
        if (localDateTime == null) {
            return null;
        }
        LocalDateTime result = localDateTime.atZone(fromZone)
            .withZoneSameInstant(toZone)
            .toLocalDateTime();
        return Timestamp.valueOf(result);
    }

    private static ZoneId parseTimeZone(@NonNull String timeZone) {
        String tz = timeZone.trim();
        if (tz.isEmpty()) {
            return null;
        }
        if ("SYSTEM".equalsIgnoreCase(tz)) {
            return ZoneId.systemDefault();
        }
        try {
            char first = tz.charAt(0);
            if (first == '+' || first == '-') {
                String offset = tz.length() == 6 ? tz : tz + ":00";
                return ZoneOffset.of(offset);
            }
            return ZoneId.of(tz);
        } catch (Exception ignore) {
            return null;
        }
    }

    private static LocalDateTime toLocalDateTime(@NonNull Object value) {
        if (value instanceof Timestamp) {
            return ((Timestamp) value).toLocalDateTime();
        }
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate().atStartOfDay();
        }
        String str = value.toString().trim();
        try {
            if (str.length() == 10) {
                return LocalDateTime.parse(str + " 00:00:00",
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            }
            return LocalDateTime.parse(str, DATE_TIME_FORMATTER);
        } catch (Exception ignore) {
            return null;
        }
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public Type getType() {
        return Types.TIMESTAMP;
    }
}
