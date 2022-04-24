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
import io.dingodb.expr.runtime.exception.FailParseTime;
import io.dingodb.expr.runtime.op.time.utils.DateFormatUtil;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
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
        return value.toString();
    }

    @Nonnull
    @Evaluators.Base(StringEvaluator.class)
    static String stringType(@Nonnull Date value) {
        return stringType(value, "yyyy-MM-dd HH:mm:ss.SSS");
    }

    @Nonnull
    @Evaluators.Base(StringEvaluator.class)
    static String stringType(@Nonnull Date value, String fmt) {
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        return sdf.format(value);
    }

    @Nonnull
    @Evaluators.Base(TimeEvaluator.class)
    static java.sql.Date time() {
        return new java.sql.Date(System.currentTimeMillis());
    }

    @Nonnull
    @Evaluators.Base(TimeEvaluator.class)
    static Date time(long timestamp) {
        return new Date(timestamp);
    }

    @Nonnull
    @Evaluators.Base(TimeEvaluator.class)
    static Date time(String str) {
        return time(str, "yyyy-MM-dd HH:mm:ss.SSS");
    }

    @Nonnull
    @Evaluators.Base(TimeEvaluator.class)
    static Date time(String str, String fmt) {
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        try {
            return sdf.parse(str);
        } catch (ParseException e) {
            throw new FailParseTime(str, fmt);
        }
    }

    @Nonnull
    @Evaluators.Base(TimestampEvaluator.class)
    static Timestamp timestamp(String str) {
        String datetime = DateFormatUtil.completeToDatetimeFormat(str);
        datetime = DateFormatUtil.validAndCompleteDateValue("TIMESTAMP", datetime);
        return Timestamp.valueOf(LocalDateTime.parse(datetime, DateFormatUtil.getDatetimeFormatter()));
    }

    @Nonnull
    @Evaluators.Base(DateEvaluator.class)
    static java.sql.Date date(String str) {
        String datetime = DateFormatUtil.completeToDatetimeFormat(str);
        String date = DateFormatUtil.validAndCompleteDateValue("DATE", datetime);
        return java.sql.Date.valueOf(LocalDate.parse(date, DateFormatUtil.getDateFormatter()));
    }
}
