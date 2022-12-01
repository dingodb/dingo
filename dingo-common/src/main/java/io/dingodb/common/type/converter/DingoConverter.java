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

import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class DingoConverter implements DataConverter {
    public static final DingoConverter INSTANCE = new DingoConverter();

    private DingoConverter() {
    }

    @Override
    public Long convert(@NonNull Date value) {
        return value.getTime();
    }

    @Override
    public Long convert(@NonNull Time value) {
        return value.getTime();
    }

    @Override
    public Long convert(@NonNull Timestamp value) {
        return value.getTime();
    }

    @Override
    public String convert(@NonNull BigDecimal value) {
        return value.toString();
    }

    @Override
    public BigDecimal convertDecimalFrom(@NonNull Object value) {
        return new BigDecimal((String) value);
    }

    @Override
    public Date convertDateFrom(@NonNull Object value) {
        return new Date((long) value);
    }

    @Override
    public Time convertTimeFrom(@NonNull Object value) {
        return new Time((long) value);
    }

    @Override
    public Timestamp convertTimestampFrom(@NonNull Object value) {
        return new Timestamp((long) value);
    }
}
