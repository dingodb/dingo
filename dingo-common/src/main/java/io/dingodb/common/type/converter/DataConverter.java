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
import org.checkerframework.checker.nullness.qual.NonNull;

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

public interface DataConverter {
    /**
     * Default converter convert nothing.
     */
    DataConverter DEFAULT = new DataConverter() {
    };

    default boolean isNull(@NonNull Object value) {
        return false;
    }

    default Object convert(@NonNull Date value) {
        return value;
    }

    default Object convert(@NonNull Time value) {
        return value;
    }

    default Object convert(@NonNull Timestamp value) {
        return value;
    }

    default Object convert(byte @NonNull [] value) {
        return value;
    }

    default Object convert(@NonNull BigDecimal value) {
        return value;
    }

    default Object convert(@NonNull Object value) {
        return value;
    }

    default Object convert(Object @NonNull [] value, @NonNull DingoType elementType) {
        return Arrays.stream(value)
            .map(v -> elementType.convertTo(v, this))
            .toArray(Object[]::new);
    }

    default Object convert(@NonNull List<?> value, @NonNull DingoType elementType) {
        return value.stream()
            .map(v -> elementType.convertTo(v, this))
            .collect(Collectors.toList());
    }

    default Object convert(@NonNull Map<?, ?> value, @NonNull DingoType keyType, @NonNull DingoType valueType) {
        Map<Object, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            result.put(
                keyType.convertTo(entry.getKey(), this),
                valueType.convertTo(entry.getValue(), this)
            );
        }
        return result;
    }

    default Integer convertIntegerFrom(@NonNull Object value) {
        return (Integer) value;
    }

    default Long convertLongFrom(@NonNull Object value) {
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        }
        return (Long) value;
    }

    default Float convertFloatFrom(@NonNull Object value) {
        return (Float) value;
    }

    default Double convertDoubleFrom(@NonNull Object value) {
        return (Double) value;
    }

    default Boolean convertBooleanFrom(@NonNull Object value) {
        return (Boolean) value;
    }

    default String convertStringFrom(@NonNull Object value) {
        return (String) value;
    }

    default BigDecimal convertDecimalFrom(@NonNull Object value) {
        return (BigDecimal) value;
    }

    default Date convertDateFrom(@NonNull Object value) {
        return (Date) value;
    }

    default Time convertTimeFrom(@NonNull Object value) {
        return (Time) value;
    }

    default Timestamp convertTimestampFrom(@NonNull Object value) {
        return (Timestamp) value;
    }

    default byte[] convertBinaryFrom(@NonNull Object value) {
        return (byte[]) value;
    }

    default Object convertObjectFrom(@NonNull Object value) {
        return value;
    }

    default Object[] convertTupleFrom(@NonNull Object value, @NonNull DingoType type) {
        Object[] tuple = (Object[]) value;
        return IntStream.range(0, tuple.length)
            .mapToObj(i -> Objects.requireNonNull(type.getChild(i)).convertFrom(tuple[i], this))
            .toArray(Object[]::new);
    }

    default Object[] convertArrayFrom(@NonNull Object value, @NonNull DingoType elementType) {
        return Arrays.stream((Object[]) value)
            .map(e -> elementType.convertFrom(e, this))
            .toArray(Object[]::new);
    }

    default List<?> convertListFrom(@NonNull Object value, @NonNull DingoType elementType) {
        return ((List<?>) value).stream()
            .map(e -> elementType.convertFrom(e, this))
            .collect(Collectors.toList());
    }

    default Map<Object, Object> convertMapFrom(
        @NonNull Object value,
        @NonNull DingoType keyType,
        @NonNull DingoType valueType
    ) {
        Map<Object, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            result.put(
                keyType.convertFrom(entry.getKey(), this),
                valueType.convertFrom(entry.getValue(), this)
            );
        }
        return result;
    }

    default Object collectTuple(@NonNull Stream<Object> stream) {
        return stream.toArray(Object[]::new);
    }
}
