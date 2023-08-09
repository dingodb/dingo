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

import io.dingodb.expr.runtime.utils.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@Slf4j
public class StrParseConverter implements DataConverter {
    public static final StrParseConverter INSTANCE = new StrParseConverter();

    private StrParseConverter() {
    }

    @Override
    public boolean isNull(@NonNull Object value) {
        return value.equals("null") || value.equals("NULL");
    }

    @Override
    public Integer convertIntegerFrom(@NonNull Object value) {
        if (value.toString().trim().isEmpty()) {
            return null;
        }
        return Integer.parseInt(value.toString());
    }

    @Override
    public Long convertLongFrom(@NonNull Object value) {
        if (value.toString().trim().isEmpty()) {
            return null;
        }
        return Long.parseLong(value.toString());
    }

    @Override
    public Float convertFloatFrom(@NonNull Object value) {
        if (value.toString().trim().isEmpty()) {
            return null;
        }
        return Float.parseFloat(value.toString());
    }

    @Override
    public Double convertDoubleFrom(@NonNull Object value) {
        if (value.toString().trim().isEmpty()) {
            return null;
        }
        return Double.parseDouble(value.toString());
    }

    @Override
    public Boolean convertBooleanFrom(@NonNull Object value) {
        String str = value.toString();
        if (str.equalsIgnoreCase("true")) {
            return Boolean.TRUE;
        }
        if (str.equalsIgnoreCase("false")) {
            return Boolean.FALSE;
        }
        if (str.trim().length() == 0) {
            log.warn("Empty string will return Null.");
            return null;
        }

        try {
            int aInt = Integer.parseInt(str);
            return aInt != 0 ? Boolean.TRUE : Boolean.FALSE;
        } catch (NumberFormatException e) {
            log.error("Failed to parse boolean value: {}", str, e);
            throw new IllegalArgumentException("Failed to parse boolean value: " + str);
        }
    }

    @Override
    public BigDecimal convertDecimalFrom(@NonNull Object value) {
        if (value.toString().trim().isEmpty()) {
            return null;
        }
        return new BigDecimal(value.toString());
    }

    @Override
    public Date convertDateFrom(@NonNull Object value) {
        String strValue = value instanceof String ? (String) value : value.toString();
        return DateTimeUtils.parseDate(strValue);
    }

    @Override
    public Time convertTimeFrom(@NonNull Object value) {
        String strValue = value instanceof String ? (String) value : value.toString();
        return DateTimeUtils.parseTime(strValue);
    }

    @Override
    public Timestamp convertTimestampFrom(@NonNull Object value) {
        String strValue = value instanceof String ? (String) value : value.toString();
        return DateTimeUtils.parseTimestamp(strValue);
    }

    @Override
    public byte[] convertBinaryFrom(@NonNull Object value) {
        String strValue = value instanceof String ? (String) value : value.toString();
        return strValue.getBytes(StandardCharsets.UTF_8);
    }
}
