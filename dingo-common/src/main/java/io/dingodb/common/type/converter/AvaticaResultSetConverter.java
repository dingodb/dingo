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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public class AvaticaResultSetConverter implements DataConverter {
    public static final AvaticaResultSetConverter INSTANCE = new AvaticaResultSetConverter();

    private AvaticaResultSetConverter() {
    }

    @Override
    public Long convert(@Nonnull Date value) {
        return Instant.ofEpochMilli(value.getTime()).atZone(ZoneOffset.UTC).toLocalDate().toEpochDay();
    }

    @Override
    public Long convert(@Nonnull Time value) {
        return value.getTime() + TimeZone.getDefault().getRawOffset();
    }

    @Override
    public Long convert(@Nonnull Timestamp value) {
        return value.getTime() + TimeZone.getDefault().getRawOffset();
    }

    @Override
    public Object collectTuple(@Nonnull Stream<Object> stream) {
        return stream.collect(Collectors.toList());
    }
}
