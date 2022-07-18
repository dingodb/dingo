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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import javax.annotation.Nonnull;

public class JsonConverter implements DataConverter {
    public static final JsonConverter INSTANCE = new JsonConverter();

    private JsonConverter() {
    }

    @Override
    public Long convert(@Nonnull Date value) {
        return value.getTime();
    }

    @Override
    public Long convert(@Nonnull Time value) {
        return value.getTime();
    }

    @Override
    public Long convert(@Nonnull Timestamp value) {
        return value.getTime();
    }

    @Override
    public Integer convertIntegerFrom(@Nonnull Object value) {
        if (value instanceof Long) {
            return ((Long) value).intValue();
        }
        return (Integer) value;
    }

    @Override
    public Date convertDateFrom(@Nonnull Object value) {
        return new Date((long) value);
    }

    @Override
    public Time convertTimeFrom(@Nonnull Object value) {
        return new Time((long) value);
    }

    @Override
    public Timestamp convertTimestampFrom(@Nonnull Object value) {
        return new Timestamp((long) value);
    }
}
