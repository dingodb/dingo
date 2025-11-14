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

import io.dingodb.expr.common.type.IntervalDayTimeType;
import io.dingodb.expr.common.type.IntervalDayType;
import io.dingodb.expr.common.type.IntervalHourType;
import io.dingodb.expr.common.type.IntervalMinuteType;
import io.dingodb.expr.common.type.IntervalMonthType;
import io.dingodb.expr.common.type.IntervalQuarterType;
import io.dingodb.expr.common.type.IntervalSecondType;
import io.dingodb.expr.common.type.IntervalType;
import io.dingodb.expr.common.type.IntervalWeekType;
import io.dingodb.expr.common.type.IntervalYearType;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.OpKeys;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;

public class DateAddFun extends BinaryOp {
    @Serial
    private static final long serialVersionUID = -1914862455684545159L;

    public static final DateAddFun INSTANCE = new DateAddFun();

    public static final String NAME = "DATE_ADD";

    @Override
    public OpKey keyOf(@NonNull Type type0, @NonNull Type type1) {
        return OpKeys.DATE_LONG.keyOf(type0, type1);
    }

    @Override
    public Object evalValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        int delta = 0;
        if (value0 instanceof String) {
            String date = value0.toString();
            Date parseDate = DateTimeUtils.parseDate(date);
            if (parseDate == null) {
                Timestamp timestamp = DateTimeUtils.parseTimestamp(date);
                if (timestamp == null) {
                    return null;
                } else {
                    value0 = timestamp;
                }
            } else {
                value0 = parseDate;
            }
        }
        if (value1 instanceof Integer) {
            delta = (int) value1;
        } else if (value1 instanceof Long) {
            Long deltaLong = (Long) value1;
            delta = deltaLong.intValue();
        } else if (value1 instanceof Float) {
            Float deltaFloat = (Float) value1;
            delta = (int) Math.round(deltaFloat);
        } else if (value1 instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) value1;
            delta = decimal.intValue();
        } else if (value1 instanceof Double) {
            Double deltaDouble = (Double) value1;
            delta = (int) Math.round(deltaDouble);
        } else if (value1 instanceof IntervalType) {
            if (value1 instanceof IntervalYearType.IntervalYear) {
                IntervalYearType.IntervalYear intervalYear = (IntervalYearType.IntervalYear) value1;
                if (intervalYear.elementType instanceof IntervalMonthType) {
                    return compute(value0, Calendar.MONTH, intervalYear.value.intValue());
                }
            } else if (value1 instanceof IntervalMonthType.IntervalMonth) {
                IntervalMonthType.IntervalMonth intervalMonth = (IntervalMonthType.IntervalMonth) value1;
                if (intervalMonth.elementType instanceof IntervalQuarterType) {
                    return compute(value0, Calendar.MONTH, intervalMonth.value.intValue() * 3);
                } else {
                    return compute(value0, Calendar.MONTH, intervalMonth.value.intValue());
                }
            } else if (value1 instanceof IntervalDayType.IntervalDay) {
                IntervalDayType.IntervalDay intervalDay = (IntervalDayType.IntervalDay) value1;
                if (intervalDay.elementType instanceof IntervalDayTimeType) {
                    long value = intervalDay.value.longValue() / (24 * 60 * 60 * 1000);
                    return compute(value0, Calendar.DAY_OF_MONTH, Math.toIntExact(value));
                }
            } else if (value1 instanceof IntervalWeekType.IntervalWeek) {
                IntervalWeekType.IntervalWeek intervalWeek = (IntervalWeekType.IntervalWeek) value1;
                if (intervalWeek.elementType instanceof IntervalDayTimeType) {
                    long value = intervalWeek.value.longValue() / (60 * 60 * 1000);
                    return compute(value0, Calendar.WEEK_OF_YEAR, Math.toIntExact(value));
                }
            } else if (value1 instanceof IntervalHourType.IntervalHour) {
                IntervalHourType.IntervalHour intervalHour = (IntervalHourType.IntervalHour) value1;
                if (intervalHour.elementType instanceof IntervalDayTimeType) {
                    long value = intervalHour.value.longValue() / (60 * 60 * 1000);
                    return compute(value0, Calendar.HOUR, Math.toIntExact(value));
                }
            } else if (value1 instanceof IntervalMinuteType.IntervalMinute) {
                IntervalMinuteType.IntervalMinute intervalMinute = (IntervalMinuteType.IntervalMinute) value1;
                if (intervalMinute.elementType instanceof IntervalDayTimeType) {
                    long value = intervalMinute.value.longValue() / (60 * 1000);
                    return compute(value0, Calendar.MINUTE, Math.toIntExact(value));
                }
            } else if (value1 instanceof IntervalSecondType.IntervalSecond) {
                IntervalSecondType.IntervalSecond intervalSecond = (IntervalSecondType.IntervalSecond) value1;
                if (intervalSecond.elementType instanceof IntervalDayTimeType) {
                    long value = intervalSecond.value.longValue() / 1000;
                    return compute(value0, Calendar.SECOND, Math.toIntExact(value));
                }
            } else {
                return null;
            }
        }

        return compute(value0, Calendar.DAY_OF_MONTH, delta);
    }

    private static Object compute(Object value0, int field, int amount) {
        if (value0 == null) {
            return null;
        }
        if (value0 instanceof Date) {
            Date date = (Date) value0;
            LocalDate l;
            LocalTime localTime = LocalTime.MIDNIGHT;
            ZoneId zoneId;
            ZonedDateTime zonedDateTime;
            Instant instant;
            switch (field) {
                case Calendar.DAY_OF_MONTH:
                case Calendar.MONTH:
                case Calendar.WEEK_OF_YEAR:
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(date);
                    calendar.add(field, amount);
                    if (calendar.get(Calendar.YEAR) > 9999) {
                        return null;
                    }
                    return new Date(calendar.getTimeInMillis());
                case Calendar.HOUR:
                    l = date.toLocalDate();
                    zoneId = ZoneId.systemDefault();
                    zonedDateTime = ZonedDateTime.of(l, localTime, zoneId).plusHours(amount);
                    instant = zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toInstant();
                    return Timestamp.from(instant);
                case Calendar.MINUTE:
                    l = date.toLocalDate();
                    zoneId = ZoneId.systemDefault();
                    zonedDateTime = ZonedDateTime.of(l, localTime, zoneId).plusMinutes(amount);
                    instant = zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toInstant();
                    return Timestamp.from(instant);
                case Calendar.SECOND:
                    l = date.toLocalDate();
                    zoneId = ZoneId.systemDefault();
                    zonedDateTime = ZonedDateTime.of(l, localTime, zoneId).plusSeconds(amount);
                    instant = zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toInstant();
                    return Timestamp.from(instant);
                default: return null;
            }
        } else if (value0 instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) value0;
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(timestamp);
            calendar.add(field, amount);
            if (calendar.get(Calendar.YEAR) > 9999) {
                return null;
            }
            return new Timestamp(calendar.getTimeInMillis());
        }
        return null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
