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

import io.dingodb.expr.common.timezone.core.DateTimeType;
import io.dingodb.expr.common.timezone.core.DingoDateTime;
import io.dingodb.expr.common.timezone.processor.DingoTimeZoneProcessor;
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
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.temporal.ChronoUnit;
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
        DingoTimeZoneProcessor processor = config.getProcessor();

        if (value1 == null) {
            return null;
        }
        long delta = 0;
        if (value0 instanceof String) {
            String date = value0.toString();
            Object processDateTime = processor.processDateTime(date, DateTimeType.DATE, DateTimeType.DATE);
            if (processDateTime == null) {
                processDateTime = processor.processDateTime(date, DateTimeType.TIMESTAMP, DateTimeType.TIMESTAMP);
                if (processDateTime == null) {
                    return null;
                } else {
                    value0 = processDateTime;
                }
            } else {
                value0 = processDateTime;
            }
        }
        if (value1 instanceof Integer) {
            delta = ((Integer) value1).longValue();
        } else if (value1 instanceof Long) {
            delta = (Long) value1;
        } else if (value1 instanceof Float) {
            Float deltaFloat = (Float) value1;
            delta = Math.round(deltaFloat);
        } else if (value1 instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) value1;
            delta = Math.round(decimal.doubleValue());
        } else if (value1 instanceof Double) {
            Double deltaDouble = (Double) value1;
            delta = Math.round(deltaDouble);
        }

        if (value1 instanceof IntervalType) {
            if (value1 instanceof IntervalYearType.IntervalYear) {
                IntervalYearType.IntervalYear intervalYear = (IntervalYearType.IntervalYear) value1;
                if (intervalYear.elementType instanceof IntervalMonthType) {
                    return compute(value0, ChronoUnit.MONTHS, intervalYear.value.longValue(), processor);
                }
            } else if (value1 instanceof IntervalMonthType.IntervalMonth) {
                IntervalMonthType.IntervalMonth intervalMonth = (IntervalMonthType.IntervalMonth) value1;
                if (intervalMonth.elementType instanceof IntervalQuarterType) {
                    return compute(value0, ChronoUnit.MONTHS, intervalMonth.value.longValue() * 3, processor);
                } else {
                    return compute(value0, ChronoUnit.MONTHS, intervalMonth.value.longValue(), processor);
                }
            } else if (value1 instanceof IntervalDayType.IntervalDay) {
                IntervalDayType.IntervalDay intervalDay = (IntervalDayType.IntervalDay) value1;
                if (intervalDay.elementType instanceof IntervalDayTimeType) {
                    long value = intervalDay.value.longValue() / (24 * 60 * 60 * 1000);
                    return compute(value0, ChronoUnit.DAYS, value, processor);
                }
            } else if (value1 instanceof IntervalWeekType.IntervalWeek) {
                IntervalWeekType.IntervalWeek intervalWeek = (IntervalWeekType.IntervalWeek) value1;
                if (intervalWeek.elementType instanceof IntervalDayTimeType) {
                    long value = intervalWeek.value.longValue() / (60 * 60 * 1000);
                    return compute(value0, ChronoUnit.WEEKS, value, processor);
                }
            } else if (value1 instanceof IntervalHourType.IntervalHour) {
                IntervalHourType.IntervalHour intervalHour = (IntervalHourType.IntervalHour) value1;
                if (intervalHour.elementType instanceof IntervalDayTimeType) {
                    long value = intervalHour.value.longValue() / (60 * 60 * 1000);
                    return compute(value0, ChronoUnit.HOURS, value, processor);
                }
            } else if (value1 instanceof IntervalMinuteType.IntervalMinute) {
                IntervalMinuteType.IntervalMinute intervalMinute = (IntervalMinuteType.IntervalMinute) value1;
                if (intervalMinute.elementType instanceof IntervalDayTimeType) {
                    long value = intervalMinute.value.longValue() / (60 * 1000);
                    return compute(value0, ChronoUnit.MINUTES, value, processor);
                }
            } else if (value1 instanceof IntervalSecondType.IntervalSecond) {
                IntervalSecondType.IntervalSecond intervalSecond = (IntervalSecondType.IntervalSecond) value1;
                if (intervalSecond.elementType instanceof IntervalDayTimeType) {
                    long value = intervalSecond.value.longValue() / 1000;
                    return compute(value0, ChronoUnit.SECONDS, value, processor);
                }
            } else {
                return null;
            }
        }

        return compute(value0, ChronoUnit.DAYS, delta, processor);
    }

    private static Object compute(Object value0, ChronoUnit unit, long amount, DingoTimeZoneProcessor processor) {
        if (value0 == null) {
            return null;
        }
        if (value0 instanceof Date) {
            switch (unit) {
                case MONTHS:
                case DAYS:
                case WEEKS:
                    DingoDateTime dateInput = processor.getTierProcessor().convertInput(value0, DateTimeType.DATE);
                    DingoDateTime dateTime = processor.dateAdd(dateInput, amount, unit);
                    Date date = (Date) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.DATE);
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(date);
                    if (calendar.get(Calendar.YEAR) > 9999) {
                        return null;
                    }
                    return date;
                case HOURS:
                case MINUTES:
                case SECONDS:
                    DingoDateTime timestampInput =
                        processor.getTierProcessor().convertInput(value0, DateTimeType.TIMESTAMP);
                    DingoDateTime timestampDateTime = processor.dateAdd(timestampInput, amount, unit);
                    Timestamp timestamp =
                        (Timestamp) processor.getTierProcessor().convertOutput(timestampDateTime, DateTimeType.TIMESTAMP);
                    Calendar instance = Calendar.getInstance();
                    instance.setTimeInMillis(timestamp.getTime());
                    if (instance.get(Calendar.YEAR) > 9999) {
                        return null;
                    }
                    return timestamp;
                default: return null;
            }
        } else if (value0 instanceof Timestamp) {
            DingoDateTime input = processor.getTierProcessor().convertInput(value0, DateTimeType.TIMESTAMP);
            DingoDateTime dateTime = processor.dateAdd(input, amount, unit);
            Timestamp timestamp = (Timestamp) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.TIMESTAMP);
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(timestamp.getTime());
            if (calendar.get(Calendar.YEAR) > 9999) {
                return null;
            }
            return timestamp;
        }
        return null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
