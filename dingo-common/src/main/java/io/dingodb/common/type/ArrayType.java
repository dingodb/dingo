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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@JsonTypeName("array")
public class ArrayType extends AbstractDingoType {
    @JsonProperty("element")
    private final DingoType elementType;
    @JsonProperty("nullable")
    private final boolean nullable;

    @JsonCreator
    ArrayType(
        @JsonProperty("element") DingoType elementType,
        @JsonProperty("nullable") boolean nullable
    ) {
        super(TypeCode.ARRAY);
        this.elementType = elementType;
        this.nullable = nullable;
    }

    @Override
    protected Object convertValueTo(@Nonnull Object value, @Nonnull DataConverter converter) {
        return converter.convert((Object[]) value, elementType);
    }

    @Override
    protected Object convertValueFrom(@Nonnull Object value, @Nonnull DataConverter converter) {
        return converter.convertArrayFrom(value, elementType);
    }

    @Override
    public DingoType copy() {
        return new ArrayType(elementType, nullable);
    }

    @Nonnull
    @Override
    public Schema toAvroSchema() {
        return Schema.createArray(elementType.toAvroSchema());
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
