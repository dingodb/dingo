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
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import io.dingodb.serial.schema.DingoSchema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

@JsonTypeName("list")
@JsonPropertyOrder({"element", "nullable"})
@EqualsAndHashCode(of = {"elementType"}, callSuper = true)
public class ListType extends NullableType {
    @Getter
    @JsonProperty("element")
    private final DingoType elementType;

    @Getter
    private final Type type;

    @JsonCreator
    ListType(
        @JsonProperty("element") @NonNull DingoType elementType,
        @JsonProperty("nullable") boolean nullable
    ) {
        super(nullable);
        this.elementType = elementType;
        this.type = Types.list(elementType.getType());
    }

    @Override
    protected Object convertValueTo(@NonNull Object value, @NonNull DataConverter converter) {
        return converter.convert((List<?>) value, elementType);
    }

    @Override
    protected Object convertValueFrom(@NonNull Object value, @NonNull DataConverter converter) {
        return converter.convertListFrom(value, elementType);
    }

    @Override
    public DingoType copy() {
        return new ListType(elementType, nullable);
    }

    @Override
    public List<DingoSchema> toDingoSchemas() {
        return null;
    }

    @Override
    public DingoSchema toDingoSchema(int index) {
        DingoSchema schema = TypeUtils.elementTypeToDingoList(elementType);
        schema.setIndex(index);
        return schema;
    }

    @Override
    public @NonNull String format(@Nullable Object value) {
        if (value != null) {
            List<?> list = (List<?>) value;
            StringBuilder b = new StringBuilder();
            b.append("[ ");
            for (int i = 0; i < list.size(); ++i) {
                if (i > 0) {
                    b.append(", ");
                }
                b.append(elementType.format(list.get(i)));
            }
            b.append(" ]");
            return b.toString();
        }
        return NullType.NULL.format(null);
    }

    @Override
    public <R, T> R accept(@NonNull DingoTypeVisitor<R, T> visitor, T obj) {
        return visitor.visitListType(this, obj);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("list(");
        b.append(elementType.toString());
        b.append(")");
        if (nullable) {
            b.append("|").append(NullType.NULL);
        }
        return b.toString();
    }
}
