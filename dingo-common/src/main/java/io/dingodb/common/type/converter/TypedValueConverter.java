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

import io.dingodb.common.type.DingoType;
import org.apache.calcite.avatica.util.DateTimeUtils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import javax.annotation.Nonnull;

public class TypedValueConverter extends ConverterWithCalendar {
    private static final RuntimeException NEVER_CONVERT_BACK
        = new IllegalStateException("Convert back to TypedValue should be avoided.");

    public TypedValueConverter(Calendar localCalendar) {
        super(localCalendar);
    }

    @Override
    public Object convert(@Nonnull Date value) {
        throw NEVER_CONVERT_BACK;
    }

    @Override
    public Object convert(@Nonnull Time value) {
        throw NEVER_CONVERT_BACK;
    }

    @Override
    public Object convert(@Nonnull Timestamp value) {
        throw NEVER_CONVERT_BACK;
    }

    @Override
    public Object convert(@Nonnull byte[] value) {
        throw NEVER_CONVERT_BACK;
    }

    @Override
    public Object convert(@Nonnull Object[] value, DingoType elementType) {
        throw NEVER_CONVERT_BACK;
    }

    /**
     * Convert from an integer of days to a {@link Date}. See {@link org.apache.calcite.avatica.remote.TypedValue}.
     *
     * @param value the days from epoch
     * @return the {@link Date}
     */
    @Override
    public Date convertDateFrom(@Nonnull Object value) {
        return new Date(((Integer) value) * DateTimeUtils.MILLIS_PER_DAY);
    }

    /**
     * Convert from an integer of milliseconds to a {@link Time}. See
     * {@link org.apache.calcite.avatica.remote.TypedValue}.
     *
     * @param value the milliseconds from epoch
     * @return the {@link Time}
     */
    @Override
    public Time convertTimeFrom(@Nonnull Object value) {
        return new Time((Integer) value);
    }

    /**
     * Convert from an integer of milliseconds to a {@link Timestamp}. See
     * {@link org.apache.calcite.avatica.remote.TypedValue}.
     *
     * @param value the milliseconds from epoch
     * @return the {@link Timestamp}
     */
    @Override
    public Timestamp convertTimestampFrom(@Nonnull Object value) {
        // Calcite's timestamp are shifted to UTC.
        return unShiftedTimestamp(((Calendar) value).getTimeInMillis());
    }
}
