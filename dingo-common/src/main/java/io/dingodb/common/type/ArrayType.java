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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.type.converter.DataConverter;
import io.dingodb.expr.core.TypeCode;
import io.dingodb.serial.schema.DingoSchema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

@JsonTypeName("array")
@JsonPropertyOrder({"element", "nullable"})
@EqualsAndHashCode(of = {"elementType"}, callSuper = true)
public class ArrayType extends NullableType {
    @Getter
    @JsonProperty("element")
    private final DingoType elementType;

    @JsonCreator
    ArrayType(
        @JsonProperty("element") DingoType elementType,
        @JsonProperty("nullable") boolean nullable
    ) {
        super(TypeCode.ARRAY, nullable);
        this.elementType = elementType;
    }

    @Override
    protected Object convertValueTo(@NonNull Object value, @NonNull DataConverter converter) {
        return converter.convert((Object[]) value, elementType);
    }

    @Override
    protected Object convertValueFrom(@NonNull Object value, @NonNull DataConverter converter) {
        return converter.convertArrayFrom(value, elementType);
    }

    @Override
    public DingoType copy() {
        return new ArrayType(elementType, nullable);
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
    public <S> @NonNull S toSchema(@NonNull SchemaConverter<S> converter) {
        return converter.createSchema(this);
    }

    @Override
    public @NonNull String format(@Nullable Object value) {
        if (value != null) {
            Object[] arr = (Object[]) value;
            StringBuilder b = new StringBuilder();
            b.append("[ ");
            for (int i = 0; i < arr.length; ++i) {
                if (i > 0) {
                    b.append(", ");
                }
                b.append(elementType.format(arr[i]));
            }
            b.append(" ]");
            return b.toString();
        }
        return NullType.NULL.format(null);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("array(");
        b.append(elementType.toString());
        b.append(")");
        if (nullable) {
            b.append("|").append(NullType.NULL);
        }
        return b.toString();
    }
}
