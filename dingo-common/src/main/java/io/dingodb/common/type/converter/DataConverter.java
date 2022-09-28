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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public interface DataConverter {
    /**
     * Default converter convert nothing.
     */
    DataConverter DEFAULT = new DataConverter() {
    };

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

    default Object convert(@Nonnull byte[] value) {
        return value;
    }

    default Object convert(@Nonnull BigDecimal value) {return value;}

    default Object convert(@Nonnull Object[] value, DingoType elementType) {
        return Arrays.stream(value)
            .map(v -> elementType.convertTo(v, this))
            .toArray(Object[]::new);
    }

    default Object convert(@Nonnull List<?> value, DingoType elementType) {
        return value.stream()
            .map(v -> elementType.convertTo(v, this))
            .collect(Collectors.toList());
    }

    default Object convert(@Nonnull Map<?, ?> value, DingoType keyType, DingoType valueType) {
        Map<Object, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            result.put(
                keyType.convertTo(entry.getKey(), this),
                valueType.convertTo(entry.getValue(), this)
            );
        }
        return result;
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

    default byte[] convertBinaryFrom(@Nonnull Object value) {
        return (byte[]) value;
    }

    default Object[] convertTupleFrom(@Nonnull Object value, @Nonnull DingoType type) {
        Object[] tuple = (Object[]) value;
        return IntStream.range(0, tuple.length)
            .mapToObj(i -> Objects.requireNonNull(type.getChild(i)).convertFrom(tuple[i], this))
            .toArray(Object[]::new);
    }

    default Object[] convertArrayFrom(@Nonnull Object value, DingoType elementType) {
        return Arrays.stream((Object[]) value)
            .map(e -> elementType.convertFrom(e, this))
            .toArray(Object[]::new);
    }

    default List<?> convertListFrom(@Nonnull Object value, DingoType elementType) {
        return ((List<?>) value).stream()
            .map(e -> elementType.convertFrom(e, this))
            .collect(Collectors.toList());
    }

    default Map<Object, Object> convertMapFrom(@Nonnull Object value, DingoType keyType, DingoType valueType) {
        Map<Object, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            result.put(
                keyType.convertFrom(entry.getKey(), this),
                valueType.convertFrom(entry.getValue(), this)
            );
        }
        return result;
    }

    default Object collectTuple(@Nonnull Stream<Object> stream) {
        return stream.toArray(Object[]::new);
    }
}
