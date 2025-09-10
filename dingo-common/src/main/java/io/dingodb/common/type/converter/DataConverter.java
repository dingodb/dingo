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

import io.dingodb.common.log.LogUtils;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.scalar.BitType;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.apache.calcite.avatica.util.ByteString;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
        if (value instanceof Integer) {
            return (Integer) value;
        } else {
            try {
                return new BigDecimal(value.toString()).intValue();
            } catch (Exception e) {
                return 0;
            }
        }
    }

    default Long convertLongFrom(@NonNull Object value) {
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else if (value instanceof Long) {
            return (Long) value;
        } else {
            try {
                return new BigDecimal(value.toString()).longValue();
            } catch (Exception e) {
                return 0L;
            }
        }
    }

    default Float convertFloatFrom(@NonNull Object value) {
        if (value instanceof Float) {
            return (Float) value;
        } else if (value instanceof Double) {
            Double valDouble = (Double) value;
            return valDouble.floatValue();
        } else {
            try {
                return new BigDecimal(value.toString()).floatValue();
            } catch (Exception e) {
                return 0F;
            }
        }
    }

    default Double convertDoubleFrom(@NonNull Object value) {
        if (value instanceof Float) {
            Float valFloat = (Float) value;
            return valFloat.doubleValue();
        } else if (value instanceof Double) {
            return (Double) value;
        } else {
            try {
                return new BigDecimal(value.toString()).doubleValue();
            } catch (Exception e) {
                return 0D;
            }
        }
    }

    default Boolean convertBooleanFrom(@NonNull Object value) {
        if (value instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) value;
            int val = bigDecimal.intValue();
            return val != 0;
        } else if (value instanceof Integer) {
            Integer intValue = (Integer) value;
            return intValue != 0;
        } else if (value instanceof Number) {
            Number number = (Number) value;
            return number.intValue() != 0;
        }
        return (Boolean) value;
    }

    default long convertBitFrom(@NonNull Object value) {
        String className = value.getClass().getName();
        long val = 0;
        if(value instanceof org.apache.calcite.avatica.util.ByteString) {
            byte[] bytes = ((ByteString) value).getBytes();

            int i = 0;
            for (; i < bytes.length; i++) {
                if(bytes[i] != 0x0) {
                    break;
                }
            }

            byte[] newBytes = new byte[8];
            System.arraycopy(bytes, i, newBytes, 8 - (bytes.length - i), bytes.length - i);
            ByteBuffer buffer = ByteBuffer.wrap(newBytes);
            val = buffer.getLong();
        } else if( value instanceof Long) {
            val = (Long) value;
        } else {
            throw new IllegalArgumentException("Unsupported value type for bit: " + value.getClass().getName());
        }
        return val;
    }

    default String convertStringFrom(@NonNull Object value) {
        if (value instanceof String) {
            return (String) value;
        } else {
            return value.toString();
        }
    }

    default BigDecimal convertDecimalFrom(@NonNull Object value, int precision, int scale) {
        return convertDecimalFrom(value);
    }

    default BigDecimal convertDecimalFrom(@NonNull Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else {
            try {
                return new BigDecimal(value.toString());
            } catch (Exception e) {
                return new BigDecimal(0);
            }
        }
    }

    default Date convertDateFrom(@NonNull Object value) {
        if (value instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) value;
            LocalDateTime localDateTime = timestamp.toLocalDateTime();
            return new Date(localDateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
        } else if (value instanceof Date) {
            return (Date) value;
        } else if (value instanceof String) {
            return DateTimeUtils.parseDate((String) value);
        } else {
            return new Date(System.currentTimeMillis());
        }
    }

    default Time convertTimeFrom(@NonNull Object value) {
        if (value instanceof Time) {
            return (Time) value;
        } else if (value instanceof Date) {
            Date date = (Date) value;
            return new Time(date.getTime());
        } else if (value instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) value;
            return new Time(timestamp.getTime());
        } else if (value instanceof String) {
            return DateTimeUtils.parseTime((String) value);
        } else {
            return new Time(System.currentTimeMillis());
        }
    }

    default Timestamp convertTimestampFrom(@NonNull Object value) {
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        } else if (value instanceof Date) {
            Date date = (Date) value;
            return new Timestamp(date.getTime());
        } else if (value instanceof String) {
            return DateTimeUtils.parseTimestamp((String) value);
        } else {
            return new Timestamp(System.currentTimeMillis());
        }
    }

    default byte[] convertBinaryFrom(@NonNull Object value) {
        byte[] bytes;
        if (value instanceof String) {
            bytes = ((String) value).getBytes();
        } else {
            bytes = (byte[])value;
        }
        return bytes;
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

    default Object convertIntervalFrom(@NonNull Object value, @NonNull Type type, Type element) {
        return null;
    }

    default Object collectTuple(@NonNull Stream<Object> stream) {
        return stream.toArray(Object[]::new);
    }
}
