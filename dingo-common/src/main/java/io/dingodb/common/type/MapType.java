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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.type.converter.DataConverter;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.serial.schema.DingoSchema;
import org.apache.avro.Schema;

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@JsonTypeName("map")
public class MapType extends AbstractDingoType {
    @JsonProperty("key")
    private final DingoType keyType;
    @JsonProperty("value")
    private final DingoType valueType;
    @JsonProperty("nullable")
    private final boolean nullable;

    @JsonCreator
    MapType(
        @JsonProperty("key") DingoType keyType,
        @JsonProperty("value") DingoType valueType,
        @JsonProperty("nullable") boolean nullable
    ) {
        super(TypeCode.LIST);
        this.keyType = keyType;
        this.valueType = valueType;
        this.nullable = nullable;
    }

    @Override
    protected Object convertValueTo(@Nonnull Object value, @Nonnull DataConverter converter) {
        return converter.convert((Map<?, ?>) value, keyType, valueType);
    }

    @Override
    protected Object convertValueFrom(@Nonnull Object value, @Nonnull DataConverter converter) {
        return converter.convertMapFrom(value, keyType, valueType);
    }

    @Override
    public DingoType copy() {
        return new MapType(keyType, valueType, nullable);
    }

    @Nonnull
    @Override
    public Schema toAvroSchema() {
        return Schema.createMap(valueType.toAvroSchema());
    }

    @Override
    public List<DingoSchema> toDingoSchemas() {
        return null;
    }

    @Override
    public DingoSchema toDingoSchema(int index) {
        return null;
    }

    @Override
    public String format(@Nullable Object value) {
        if (value != null) {
            Map<?, ?> map = (Map<?, ?>) value;
            StringBuilder b = new StringBuilder();
            b.append("[ ");
            int i = 0;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (i > 0) {
                    b.append(", ");
                }
                b.append(entry.getKey());
                b.append(": ");
                b.append(entry.getValue());
                ++i;
            }
            b.append(" ]");
            return b.toString();
        }
        return NullType.NULL.format(null);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("map(");
        b.append(keyType.toString());
        b.append(", ");
        b.append(valueType.toString());
        b.append(")");
        if (nullable) {
            b.append("|").append(NullType.NULL);
        }
        return b.toString();
    }
}
