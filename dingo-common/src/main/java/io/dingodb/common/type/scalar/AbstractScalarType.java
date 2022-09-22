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

package io.dingodb.common.type.scalar;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.common.type.AbstractDingoType;
import io.dingodb.common.type.NullType;
import io.dingodb.common.type.converter.DataConverter;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.serial.schema.DingoSchema;
import lombok.EqualsAndHashCode;
import org.apache.avro.Schema;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@EqualsAndHashCode(of = {"nullable"}, callSuper = true)
public abstract class AbstractScalarType extends AbstractDingoType {
    @JsonProperty(value = "nullable", defaultValue = "false")
    @JsonInclude(value = JsonInclude.Include.NON_DEFAULT)
    protected final Boolean nullable;

    protected AbstractScalarType(int typeCode, boolean nullable) {
        super();
        this.typeCode = typeCode;
        this.nullable = nullable;
    }

    @Nonnull
    @Override
    public Schema toAvroSchema() {
        Schema.Type t = getAvroSchemaType();
        if (nullable) {
            // Allow avro to encode `null`.
            return Schema.createUnion(Schema.create(t), Schema.create(Schema.Type.NULL));
        } else {
            return Schema.create(t);
        }
    }

    @Override
    public List<DingoSchema> toDingoSchemas() {
        return null;
    }

    @Override
    public String format(@Nullable Object value) {
        return value != null ? value + ":" + this : NullType.NULL.format(null);
    }

    @Override
    protected Object convertValueTo(@Nonnull Object value, @Nonnull DataConverter converter) {
        return value;
    }

    @Override
    protected Object convertValueFrom(@Nonnull Object value, @Nonnull DataConverter converter) {
        return value;
    }

    @Override
    public int fieldCount() {
        return -1;
    }

    @Override
    public String toString() {
        String name = TypeCode.nameOf(typeCode);
        return nullable ? name + "|" + NullType.NULL : name;
    }

    protected abstract Schema.Type getAvroSchemaType();
}
