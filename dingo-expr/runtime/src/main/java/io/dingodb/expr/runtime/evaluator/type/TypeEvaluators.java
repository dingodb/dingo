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

package io.dingodb.expr.runtime.evaluator.type;

import io.dingodb.expr.annotations.Evaluators;
import io.dingodb.expr.runtime.evaluator.base.BooleanEvaluator;
import io.dingodb.expr.runtime.evaluator.base.DateEvaluator;
import io.dingodb.expr.runtime.evaluator.base.DecimalEvaluator;
import io.dingodb.expr.runtime.evaluator.base.DoubleEvaluator;
import io.dingodb.expr.runtime.evaluator.base.Evaluator;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorKey;
import io.dingodb.expr.runtime.evaluator.base.IntegerEvaluator;
import io.dingodb.expr.runtime.evaluator.base.LongEvaluator;
import io.dingodb.expr.runtime.evaluator.base.StringEvaluator;
import io.dingodb.expr.runtime.evaluator.base.TimeEvaluator;
import io.dingodb.expr.runtime.evaluator.base.TimestampEvaluator;
import io.dingodb.expr.runtime.evaluator.base.UniversalEvaluator;
import io.dingodb.expr.runtime.evaluator.utils.Time2StringUtils;
import io.dingodb.expr.runtime.exception.FailParseTime;
import io.dingodb.expr.runtime.op.time.utils.DateFormatUtil;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import javax.annotation.Nonnull;


@Evaluators(
    evaluatorKey = EvaluatorKey.class,
    evaluatorBase = Evaluator.class,
    evaluatorFactory = EvaluatorFactory.class,
    universalEvaluator = UniversalEvaluator.class,
    induceSequence = {}
)
final class TypeEvaluators {
    private TypeEvaluators() {
    }

    @Evaluators.Base(IntegerEvaluator.class)
    static int intType(int value) {
        return value;
    }

    @Evaluators.Base(IntegerEvaluator.class)
    static int intType(long value) {
        return (int) value;
    }

    @Evaluators.Base(IntegerEvaluator.class)
    static int intType(double value) {
        return (int) value;
    }

    @Evaluators.Base(IntegerEvaluator.class)
    static int intType(@Nonnull BigDecimal value) {
        return value.intValue();
    }

    @Evaluators.Base(IntegerEvaluator.class)
    static int intType(String value) {
        return Integer.parseInt(value);
    }

    @Evaluators.Base(LongEvaluator.class)
    static long longType(int value) {
        return value;
    }

    @Evaluators.Base(LongEvaluator.class)
    static long longType(long value) {
        return value;
    }

    @Evaluators.Base(LongEvaluator.class)
    static long longType(double value) {
        return (long) value;
    }

    @Evaluators.Base(LongEvaluator.class)
    static long longType(@Nonnull BigDecimal value) {
        return value.longValue();
    }

    @Evaluators.Base(LongEvaluator.class)
    static long longType(@Nonnull String value) {
        return Long.parseLong(value);
    }

    @Evaluators.Base(LongEvaluator.class)
    static long longType(@Nonnull Date value) {
        return value.getTime();
    }

    @Evaluators.Base(DoubleEvaluator.class)
    static double doubleType(int value) {
        return value;
    }

    @Evaluators.Base(DoubleEvaluator.class)
    static double doubleType(long value) {
        return value;
    }

    @Evaluators.Base(DoubleEvaluator.class)
    static double doubleType(double value) {
        return value;
    }

    @Evaluators.Base(DoubleEvaluator.class)
    static double doubleType(@Nonnull BigDecimal value) {
        return value.doubleValue();
    }

    @Evaluators.Base(DoubleEvaluator.class)
    static double doubleType(@Nonnull String value) {
        return Double.parseDouble(value);
    }

    @Nonnull
    @Evaluators.Base(DecimalEvaluator.class)
    static BigDecimal decimalType(int value) {
        return BigDecimal.valueOf(value);
    }

    @Nonnull
    @Evaluators.Base(DecimalEvaluator.class)
    static BigDecimal decimalType(long value) {
        return BigDecimal.valueOf(value);
    }

    @Nonnull
    @Evaluators.Base(DecimalEvaluator.class)
    static BigDecimal decimalType(double value) {
        return BigDecimal.valueOf(value);
    }

    @Nonnull
    @Evaluators.Base(DecimalEvaluator.class)
    static BigDecimal decimalType(BigDecimal value) {
        return value;
    }

    @Nonnull
    @Evaluators.Base(DecimalEvaluator.class)
    static BigDecimal decimalType(String value) {
        return new BigDecimal(value);
    }

    @Evaluators.Base(StringEvaluator.class)
    static String stringType(@Nonnull Object value) {
        if (value instanceof java.sql.Time) {
            return Time2StringUtils.convertTime2String((java.sql.Time) value);
        } else if (value instanceof java.sql.Timestamp) {
            return Time2StringUtils.convertTimeStamp2String((java.sql.Timestamp) value, "yyyy-MM-dd HH:mm:ss");
        } else if (value instanceof java.sql.Date) {
            return Time2StringUtils.convertDate2String((java.sql.Date) value);
        } else if (value instanceof Long) {
            return stringType((Long) value);
        } else {
            return value.toString();
        }
    }

    @Evaluators.Base(StringEvaluator.class)
    static String stringType(@Nonnull Long value) {
        int exepectMSLen = String.valueOf(System.currentTimeMillis()).length();
        int inputValueLen = String.valueOf(value).length();
        if (inputValueLen == exepectMSLen || inputValueLen == exepectMSLen - 3) {
            Long timeStampWithSeconds = (inputValueLen == exepectMSLen) ? value / 1000 : value;

            /**
             * very trick mode: to check current timestamp is date or timestamp.
             */
            if (timeStampWithSeconds % (24 * 60 * 60L) != 0) {
                Timestamp timestamp = new Timestamp(timeStampWithSeconds);
                return Time2StringUtils.convertTimeStamp2String(timestamp, "yyyy-MM-dd HH:mm:ss");
            } else {
                java.sql.Date dateValue = new java.sql.Date(timeStampWithSeconds * 1000);
                return Time2StringUtils.convertDate2String(dateValue);
            }
        } else if (inputValueLen < exepectMSLen - 3) {
            java.sql.Time time = new java.sql.Time(value);
            return Time2StringUtils.convertTime2String(time);
        }
        return value.toString();
    }

    @Nonnull
    @Evaluators.Base(StringEvaluator.class)
    static String stringType(@Nonnull Date value) {
        return stringType(value, "yyyy-MM-dd");
    }

    @Nonnull
    @Evaluators.Base(StringEvaluator.class)
    static String stringType(@Nonnull Date value, String fmt) {
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        return sdf.format(value);
    }

    @Nonnull
    @Evaluators.Base(TimeEvaluator.class)
    static Time timeType(String str) {
        LocalTime time = LocalTime.parse(str);
        return new Time(((time.getHour() * 60 + time.getMinute()) * 60 + time.getSecond()) * 1000
            + time.getNano() / 1000000);
    }

    @Nonnull
    @Evaluators.Base(TimestampEvaluator.class)
    static Timestamp timestampType(String str) {
        LocalDateTime datetime;
        try {
            datetime = DateFormatUtil.convertToDatetime(str);
        } catch (SQLException e) {
            throw new FailParseTime(e.getMessage().split("FORMAT")[0], e.getMessage().split("FORMAT")[1]);
        }
        return Timestamp.valueOf(datetime);
    }

    @Nonnull
    @Evaluators.Base(DateEvaluator.class)
    static Date dateType(String str) {
        LocalDate date;
        try {
            date = DateFormatUtil.convertToDate(str);
        } catch (SQLException e) {
            throw new FailParseTime(e.getMessage().split("FORMAT")[0], e.getMessage().split("FORMAT")[1]);
        }
        return new Date(date.toEpochDay() * 24 * 60 * 60 * 1000);
    }

    @Evaluators.Base(BooleanEvaluator.class)
    static boolean booleanType(@Nonnull Object value) {
        if (value instanceof Number) {
            Double doubleValue = Double.valueOf(value.toString());
            if (doubleValue == 0) {
                return false;
            } else if (doubleValue < 0) {
                throw new RuntimeException("Invalid input parameter.");
            } else {
                if (doubleValue % 1 != 0) {
                    throw new RuntimeException("Invalid input parameter.");
                }
                return true;
            }
        }

        if (value instanceof Boolean) {
            return (boolean) value;
        }

        throw new RuntimeException("Invalid input parameter.");
    }
}
