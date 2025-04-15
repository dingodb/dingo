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

package io.dingodb.exec.fun;

import io.dingodb.common.log.LogUtils;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.OpKeys;
import io.dingodb.expr.runtime.op.cast.DateCastOpFactory;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@Slf4j
public class StrToDateFun extends BinaryOp {
    @Serial
    private static final long serialVersionUID = 8883906487235211037L;

    public static final StrToDateFun INSTANCE = new StrToDateFun();

    public static final String NAME = "str_to_date";

    @Override
    public OpKey keyOf(@NonNull Type type0, @NonNull Type type1) {
        return OpKeys.ALL_STRING.keyOf(type0, type1);
    }

    @Override
    public Object evalValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        if (value0 == null) {
            return null;
        }
        String strVal = value0.toString();
        try {
            Date date = DateTimeUtils.parseDate(strVal, DateTimeUtils.DEFAULT_PARSE_DATE_FORMATTERS);
            return date;
        } catch (Exception ignored) {
        }
        try {
            return DateTimeUtils.parseDate(strVal, DateTimeUtils.DEFAULT_PARSE_TIMESTAMP_FORMATTERS);
        } catch (Exception e) {
            LogUtils.info(log, "str to date parse timestamp: {} error", strVal);
        }
        DateCastOpFactory s;
        try {
            return DateTimeUtils.parseDate(strVal, DateTimeUtils.DEFAULT_PARSE_TIME_FORMATTERS);
        } catch (Exception e) {
            LogUtils.info(log, "str to date parse time: {} error", strVal);
        }
        String format = value1.toString();
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(format);
        try {
            LocalDateTime t = LocalDate.parse(strVal, dateTimeFormatter).atStartOfDay();
            return new Date(t.toInstant(ZoneOffset.UTC).toEpochMilli());
        } catch (DateTimeParseException ignored) {
        }
        return null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
