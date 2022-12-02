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

package io.dingodb.driver.type.converter;

import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.scalar.TimestampType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AvaticaResultSetConverter extends ConverterWithCalendar {
    public AvaticaResultSetConverter(Calendar localCalendar) {
        super(localCalendar);
    }

    /**
     * Convert a timestamp value to the proper value required in a {@link java.sql.ResultSet}.
     * <p>
     * NOTE: The value is subtracted by time offset of local calendar when accessed, see `TimestampAccessor` class in
     * {@link org.apache.calcite.avatica.util.AbstractCursor}. So we add it back to recover the original value.
     *
     * @param value the input timestamp value
     * @return the output timestamp value
     */
    @Override
    public Timestamp convert(@NonNull Timestamp value) {
        // NOTE: The following is not exact the inversion of what done in `TimestampAccessor`.
        return shiftedTimestamp(value.getTime());
    }

    @Override
    public Object convert(@NonNull Object value) {
        return value;
    }

    @Override
    public Object convert(@NonNull List<?> value, @NonNull DingoType elementType) {
        if (elementType instanceof TimestampType) {
            // Timestamps in array is not shifted by Calcite.
            return value;
        }
        return super.convert(value, elementType);
    }

    @Override
    public Object collectTuple(@NonNull Stream<Object> stream) {
        return stream.collect(Collectors.toList());
    }
}
