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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import javax.annotation.Nonnull;

public class CsvConverter implements DataConverter {
    public static final CsvConverter INSTANCE = new CsvConverter();

    private CsvConverter() {
    }

    @Override
    public boolean isNull(@Nonnull Object value) {
        return value.equals("null") || value.equals("NULL");
    }

    @Override
    public Integer convertIntegerFrom(@Nonnull Object value) {
        return Integer.parseInt((String) value);
    }

    @Override
    public Long convertLongFrom(@Nonnull Object value) {
        return Long.parseLong((String) value);
    }

    @Override
    public Double convertDoubleFrom(@Nonnull Object value) {
        return Double.parseDouble((String) value);
    }

    @Override
    public Boolean convertBooleanFrom(@Nonnull Object value) {
        String str = (String) value;
        try {
            int aInt = Integer.parseInt(str);
            return aInt != 0 ? Boolean.TRUE : Boolean.FALSE;
        } catch (NumberFormatException ignored) {
        }
        if (str.equalsIgnoreCase("true")) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    @Override
    public BigDecimal convertDecimalFrom(@Nonnull Object value) {
        return new BigDecimal((String) value);
    }

    @Override
    public Date convertDateFrom(@Nonnull Object value) {
        return DateTimeUtils.parseDate((String) value);
    }

    @Override
    public Time convertTimeFrom(@Nonnull Object value) {
        return DateTimeUtils.parseTime((String) value);
    }

    @Override
    public Timestamp convertTimestampFrom(@Nonnull Object value) {
        return DateTimeUtils.parseTimestamp((String) value);
    }
}
