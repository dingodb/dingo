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

package io.dingodb.common.type.scalar;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.time.DingoTimeZoneContext;
import io.dingodb.common.type.DingoTypeVisitor;
import io.dingodb.common.type.NullType;
import io.dingodb.common.type.converter.DataConverter;
import io.dingodb.expr.common.timezone.core.DateTimeType;
import io.dingodb.expr.common.type.Types;
import io.dingodb.serial.schema.DingoSchema;
import io.dingodb.serial.schema.LongSchema;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;

@Slf4j
@JsonTypeName("time")
public class TimeType extends AbstractScalarType {
    @JsonCreator
    public TimeType(@JsonProperty("nullable") boolean nullable) {
        super(Types.TIME, nullable);
    }

    @Override
    public TimeType copy() {
        return new TimeType(nullable);
    }

    @Override
    public DingoSchema toDingoSchema(int index) {
        return new LongSchema(index);
    }

    @Override
    public @NonNull String format(@Nullable Object value) {
        return value != null ? DingoTimeZoneContext.getProcessor()
            .formatDateTime((Time) value, DateTimeFormatter.ISO_LOCAL_TIME) + ":" + this
            : NullType.NULL.format(null);
    }

    @Override
    public <R, T> R accept(@NonNull DingoTypeVisitor<R, T> visitor, T obj) {
        return visitor.visitTimeType(this, obj);
    }

    @Override
    protected Object convertValueTo(@NonNull Object value, @NonNull DataConverter converter) {
        if (value instanceof Time) {
            return converter.convert((Time) value);
        } else if (value instanceof Timestamp) {
            return converter.convert(DingoTimeZoneContext.getProcessor().processDateTime(value, DateTimeType.TIME));
        } else if (value instanceof Date) {
            return converter.convert(DingoTimeZoneContext.getProcessor().processDateTime(value, DateTimeType.TIME));
        } else if (value instanceof Long) {
            return converter.convert(new Time((Long) value));
        } else {
            if (value != null) {
                LogUtils.error(log, "time value:{} convertValueTo error", value);
            }
            return null;
        }
    }

    @Override
    protected Object convertValueFrom(@NonNull Object value, @NonNull DataConverter converter) {
        return converter.convertTimeFrom(value);
    }
}
