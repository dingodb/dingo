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

package io.dingodb.common.type;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public interface DataConverter {
    default boolean isNull(@Nonnull Object value) {
        return false;
    }

    default Object convert(@Nonnull Date value) {
        return value;
    }

    default Object convert(@Nonnull Time value) {
        return value;
    }

    default Object convert(@Nonnull Timestamp value) {
        return value;
    }

    default Integer convertIntegerFrom(@Nonnull Object value) {
        return (Integer) value;
    }

    default Long convertLongFrom(@Nonnull Object value) {
        return (Long) value;
    }

    default Double convertDoubleFrom(@Nonnull Object value) {
        return (Double) value;
    }

    default Boolean convertBooleanFrom(@Nonnull Object value) {
        return (Boolean) value;
    }

    default String convertStringFrom(@Nonnull Object value) {
        return (String) value;
    }

    default BigDecimal convertDecimalFrom(@Nonnull Object value) {
        return (BigDecimal) value;
    }

    default Date convertDateFrom(@Nonnull Object value) {
        return (Date) value;
    }

    default Time convertTimeFrom(@Nonnull Object value) {
        return (Time) value;
    }

    default Timestamp convertTimestampFrom(@Nonnull Object value) {
        return (Timestamp) value;
    }

    default Object collectTuple(@Nonnull Stream<Object> stream) {
        return stream.toArray(Object[]::new);
    }
}
