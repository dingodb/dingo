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

package io.dingodb.common.type.converter;

import io.dingodb.common.type.DataConverter;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import javax.annotation.Nonnull;

public class RexLiteralConverter implements DataConverter {
    public static final RexLiteralConverter INSTANCE = new RexLiteralConverter();

    private RexLiteralConverter() {

    }

    @Override
    public Object convert(@Nonnull Date value) {
        return new DateString(DateTimeUtils.dateFormat(value));
    }

    @Override
    public Object convert(@Nonnull Time value) {
        return TimeString.fromMillisOfDay((int) value.getTime());
    }

    @Override
    public Object convert(@Nonnull Timestamp value) {
        return TimestampString.fromMillisSinceEpoch(value.getTime());
    }

    @Override
    public Integer convertIntegerFrom(@Nonnull Object value) {
        long v = ((BigDecimal) value).setScale(0, RoundingMode.HALF_UP).longValue();
        if ((int) v != v) {
            throw new ArithmeticException(
                "Value " + value + " exceeds limits of integer, "
                    + "which is from " + Integer.MIN_VALUE + " to " + Integer.MAX_VALUE + "."
            );
        }
        return (int) v;
    }

    @Override
    public Long convertLongFrom(@Nonnull Object value) {
        return ((BigDecimal) value).setScale(0, RoundingMode.HALF_UP).longValue();
    }

    @Override
    public Double convertDoubleFrom(@Nonnull Object value) {
        return ((BigDecimal) value).doubleValue();
    }

    @Override
    public String convertStringFrom(@Nonnull Object value) {
        return ((NlsString) value).getValue();
    }

    @Override
    public Date convertDateFrom(@Nonnull Object value) {
        return new Date(((Calendar) value).getTimeInMillis());
    }

    @Override
    public Time convertTimeFrom(@Nonnull Object value) {
        return new Time(((Calendar) value).getTimeInMillis());
    }

    @Override
    public Timestamp convertTimestampFrom(@Nonnull Object value) {
        return new Timestamp(((Calendar) value).getTimeInMillis());
    }
}
