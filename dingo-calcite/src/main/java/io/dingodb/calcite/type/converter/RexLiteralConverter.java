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

package io.dingodb.calcite.type.converter;

import io.dingodb.common.time.DingoTimeZoneContext;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.DataConverter;
import io.dingodb.expr.common.timezone.DateTimeUtils;
import io.dingodb.expr.common.timezone.core.DateTimeType;
import io.dingodb.expr.common.timezone.processor.DingoTimeZoneProcessor;
import io.dingodb.expr.common.type.IntervalDayTimeType;
import io.dingodb.expr.common.type.IntervalDayType;
import io.dingodb.expr.common.type.IntervalHourType;
import io.dingodb.expr.common.type.IntervalMinuteType;
import io.dingodb.expr.common.type.IntervalMonthType;
import io.dingodb.expr.common.type.IntervalQuarterType;
import io.dingodb.expr.common.type.IntervalSecondType;
import io.dingodb.expr.common.type.IntervalWeekType;
import io.dingodb.expr.common.type.IntervalYearType;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.expr.Exprs;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.util.NlsString;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class RexLiteralConverter implements DataConverter {
    public static final RexLiteralConverter INSTANCE = new RexLiteralConverter();
    private static final RuntimeException NEVER_CONVERT_BACK
        = new IllegalStateException("Convert back to RexLiteral should be avoided.");

    private RexLiteralConverter() {
    }

    @Override
    public Object convert(@NonNull Date value) {
        throw NEVER_CONVERT_BACK;
    }

    @Override
    public Object convert(@NonNull Time value) {
        throw NEVER_CONVERT_BACK;
    }

    @Override
    public Object convert(@NonNull Timestamp value) {
        throw NEVER_CONVERT_BACK;
    }

    @Override
    public Object convert(byte @NonNull [] value) {
        throw NEVER_CONVERT_BACK;
    }

    @Override
    public Object convert(Object @NonNull [] value, @NonNull DingoType elementType) {
        throw NEVER_CONVERT_BACK;
    }

    @Override
    public Integer convertIntegerFrom(@NonNull Object value) {
        return (Integer) ExprCompiler.ADVANCED.visit(Exprs.op(Exprs.TO_INT_C, Exprs.val(value))).eval();
    }

    @Override
    public Long convertLongFrom(@NonNull Object value) {
        return (Long) ExprCompiler.ADVANCED.visit(Exprs.op(Exprs.TO_LONG_C, Exprs.val(value))).eval();
    }

    @Override
    public Float convertFloatFrom(@NonNull Object value) {
        if (value instanceof Float) {
            return (Float) value;
        } else if (value instanceof Double) {
            Double valDouble = (Double) value;
            return valDouble.floatValue();
        } else if (value instanceof NlsString) {
            return new BigDecimal(((NlsString) value).getValue()).floatValue();
        } else {
            try {
                return new BigDecimal(value.toString()).floatValue();
            } catch (Exception e) {
                return 0F;
            }
        }
        //return ((BigDecimal) value).floatValue();
    }

    @Override
    public Double convertDoubleFrom(@NonNull Object value) {
        if (value instanceof Float) {
            Float valFloat = (Float) value;
            return valFloat.doubleValue();
        } else if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof NlsString) {
            return new BigDecimal(((NlsString) value).getValue()).doubleValue();
        } else {
            try {
                return new BigDecimal(value.toString()).doubleValue();
            } catch (Exception e) {
                return 0D;
            }
        }
    }

    @Override
    public String convertStringFrom(@NonNull Object value) {
        if (value instanceof NlsString) {
            return ((NlsString) value).getValue();
        } else if (value instanceof Timestamp) {
            return DateTimeUtils.timestampFormat((Timestamp) value);
        } else {
            return value.toString();
        }
    }

    @Override
    public Date convertDateFrom(@NonNull Object value) {
        if (value instanceof NlsString) {
            NlsString nlsString = (NlsString) value;
            String val = nlsString.getValue();
            return (Date) DingoTimeZoneContext.getProcessor().processDateTime(val, DateTimeType.DATE);
        } else {
            Calendar calendar = (Calendar) value;
            calendar.setTimeZone(DingoTimeZoneContext.getTimeZone());
            long v = calendar.getTimeInMillis();
            return new Date(v - calendar.getTimeZone().getOffset(v));
        }
    }

    @Override
    public Time convertTimeFrom(@NonNull Object value) {
        DingoTimeZoneProcessor processor = DingoTimeZoneContext.getProcessor();

        if (value instanceof NlsString) {
            String valStr = ((NlsString) value).getValue();
            return (Time) processor.processDateTime(valStr, DateTimeType.TIME);
        } else if (value instanceof Calendar) {
            Calendar calendar = (Calendar) value;
            calendar.setTimeZone(DingoTimeZoneContext.getTimeZone());
            long v = calendar.getTimeInMillis();
            return new Time(v - calendar.getTimeZone().getOffset(v));
        } else if (value instanceof Time) {
            return (Time) value;
        } else if (value instanceof Timestamp) {
            return (Time) processor.processDateTime(value, DateTimeType.TIMESTAMP, DateTimeType.TIME);
        } else if (value instanceof Date) {
            return (Time) processor.processDateTime(value, DateTimeType.DATE, DateTimeType.TIME);
        } else if (value instanceof Number) {
            Number number = (Number) value;
            try {
                return new Time(number.longValue());
            } catch (Exception e) {
                return null;
            }
        } else {
            return (Time) processor.processDateTime(value, DateTimeType.TIME);
        }
    }

    @Override
    public Timestamp convertTimestampFrom(@NonNull Object value) {
        // This works for literal like `TIMESTAMP '1970-01-01 00:00:00'`, which returns UTC time, not local time
        if (value instanceof NlsString) {
            NlsString nlsString = (NlsString) value;
            String val = nlsString.getValue();
            return (Timestamp) DingoTimeZoneContext.getProcessor().processDateTime(val, DateTimeType.TIMESTAMP);
        }
        Calendar calendar = (Calendar) value;
        calendar.setTimeZone(DingoTimeZoneContext.getTimeZone());
        long v = calendar.getTimeInMillis();
        return new Timestamp(v - calendar.getTimeZone().getOffset(v));
    }

    @Override
    public byte[] convertBinaryFrom(@NonNull Object value) {
        if( value instanceof NlsString) {
            return ((NlsString)value).getValue().getBytes();
        }
        return ((ByteString) value).getBytes();
    }

    @Override
    public Type convertIntervalFrom(@NonNull Object value, @NonNull Type type, Type element) {
        return classType(convertDecimalFrom(value), type, element);
    }

    private static Type classType(BigDecimal value, Type type, Type element) {
        if (IntervalYearType.class.isAssignableFrom(type.getClass())) {
            if (element != null && IntervalMonthType.class.isAssignableFrom(element.getClass())) {
                return new IntervalYearType.IntervalYear(value, element);
            } else {
                return new IntervalYearType.IntervalYear(value, type);
            }
        } else if (IntervalMonthType.class.isAssignableFrom(type.getClass())) {
            if (element != null && IntervalQuarterType.class.isAssignableFrom(element.getClass())) {
                return new IntervalMonthType.IntervalMonth(value, element);
            } else {
                return new IntervalMonthType.IntervalMonth(value, type);
            }
        } else if (IntervalDayType.class.isAssignableFrom(type.getClass())) {
            if (element != null && IntervalDayTimeType.class.isAssignableFrom(element.getClass())) {
                return new IntervalDayType.IntervalDay(value, element);
            } else {
                return new IntervalDayType.IntervalDay(value, type);
            }
        } else if (IntervalWeekType.class.isAssignableFrom(type.getClass())) {
            if (element != null && IntervalDayTimeType.class.isAssignableFrom(element.getClass())) {
                return new IntervalWeekType.IntervalWeek(value, element);
            } else {
                return new IntervalWeekType.IntervalWeek(value, type);
            }
        } else if (IntervalHourType.class.isAssignableFrom(type.getClass())) {
            if (element != null && IntervalDayTimeType.class.isAssignableFrom(element.getClass())) {
                return new IntervalHourType.IntervalHour(value, element);
            } else {
                return new IntervalHourType.IntervalHour(value, type);
            }
        } else if (IntervalMinuteType.class.isAssignableFrom(type.getClass())) {
            if (element != null && IntervalDayTimeType.class.isAssignableFrom(element.getClass())) {
                return new IntervalMinuteType.IntervalMinute(value, element);
            } else {
                return new IntervalMinuteType.IntervalMinute(value, type);
            }
        } else if (IntervalSecondType.class.isAssignableFrom(type.getClass())) {
            if (element != null && IntervalDayTimeType.class.isAssignableFrom(element.getClass())) {
                return new IntervalSecondType.IntervalSecond(value, element);
            } else {
                return new IntervalSecondType.IntervalSecond(value, type);
            }
        }
        return null;
    }
}
