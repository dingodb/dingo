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

package io.dingodb.exec.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.DataConverter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

public class JsonConverter implements DataConverter {
    public static final JsonConverter INSTANCE = new JsonConverter();

    private JsonConverter() {
    }

    @Override
    public boolean isNull(@NonNull Object value) {
        return value instanceof NullNode;
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
    public String convert(byte @NonNull [] value) {
        return Base64.getEncoder().encodeToString(value);
    }

    @Override
    public Integer convertIntegerFrom(@NonNull Object value) {
        return ((JsonNode) value).intValue();
    }

    @Override
    public Long convertLongFrom(@NonNull Object value) {
        if (value instanceof Long) {
            return (Long) value;
        }
        return ((JsonNode) value).longValue();
    }

    @Override
    public Float convertFloatFrom(@NonNull Object value) {
        return ((JsonNode) value).floatValue();
    }

    @Override
    public Double convertDoubleFrom(@NonNull Object value) {
        return ((JsonNode) value).doubleValue();
    }

    @Override
    public Boolean convertBooleanFrom(@NonNull Object value) {
        return ((JsonNode) value).booleanValue();
    }

    @Override
    public String convertStringFrom(@NonNull Object value) {
        return ((JsonNode) value).asText();
    }

    @Override
    public BigDecimal convertDecimalFrom(@NonNull Object value) {
        return ((JsonNode) value).decimalValue();
    }

    @Override
    public Date convertDateFrom(@NonNull Object value) {
        return new Date(((JsonNode) value).longValue());
    }

    @Override
    public Time convertTimeFrom(@NonNull Object value) {
        return new Time(((JsonNode) value).longValue());
    }

    @Override
    public Timestamp convertTimestampFrom(@NonNull Object value) {
        return new Timestamp(((JsonNode) value).longValue());
    }

    @Override
    public byte[] convertBinaryFrom(@NonNull Object value) {
        return Base64.getDecoder().decode(((JsonNode) value).asText());
    }

    @Override
    public Object[] convertTupleFrom(@NonNull Object value, @NonNull DingoType type) {
        ArrayNode arrayNode = (ArrayNode) value;
        return IntStream.range(0, arrayNode.size())
            .mapToObj(i -> Objects.requireNonNull(type.getChild(i)).convertFrom(arrayNode.get(i), this))
            .toArray(Object[]::new);
    }

    @Override
    public Object[] convertArrayFrom(@NonNull Object value, @NonNull DingoType elementType) {
        ArrayNode arrayNode = (ArrayNode) value;
        Object[] tuple = new Object[arrayNode.size()];
        for (int i = 0; i < tuple.length; ++i) {
            tuple[i] = elementType.convertFrom(arrayNode.get(i), this);
        }
        return tuple;
    }

    @Override
    public List<?> convertListFrom(@NonNull Object value, @NonNull DingoType elementType) {
        ArrayNode arrayNode = (ArrayNode) value;
        List<Object> list = new ArrayList<>(arrayNode.size());
        for (JsonNode node : arrayNode) {
            list.add(elementType.convertFrom(node, this));
        }
        return list;
    }

    @Override
    public Map<Object, Object> convertMapFrom(
        @NonNull Object value,
        @NonNull DingoType keyType,
        @NonNull DingoType valueType
    ) {
        ObjectNode objectNode = (ObjectNode) value;
        Map<Object, Object> map = new LinkedHashMap<>();
        for (Iterator<Map.Entry<String, JsonNode>> it = objectNode.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = it.next();
            map.put(
                keyType.parse(entry.getKey()), // This is a string.
                valueType.convertFrom(entry.getValue(), this)
            );
        }
        return map;
    }
}
