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

import com.fasterxml.jackson.annotation.JsonValue;
import io.dingodb.common.type.AbstractDingoType;
import io.dingodb.common.type.DataConverter;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.serial.schema.DingoSchema;
import lombok.EqualsAndHashCode;
import org.apache.avro.Schema;

import javax.annotation.Nonnull;
import java.util.List;

@EqualsAndHashCode(of = {"nullable"}, callSuper = true)
public abstract class AbstractScalarType extends AbstractDingoType {
    protected final boolean nullable;

    protected AbstractScalarType(int typeCode, boolean nullable) {
        super(typeCode);
        this.nullable = nullable;
    }

    @Override
    public int fieldCount() {
        return -1;
    }

    @Override
    public DingoType getChild(@Nonnull Object index) {
        return null;
    }

    @Override
    public DingoType select(@Nonnull TupleMapping mapping) {
        throw new IllegalStateException("Selecting fields from a scalar type is stupid.");
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
    public String format(@Nonnull Object value) {
        return value + ":" + this;
    }

    @Override
    protected Object convertValueTo(@Nonnull Object value, @Nonnull DataConverter converter) {
        return value;
    }

    @Override
    protected Object convertValueFrom(@Nonnull Object value, @Nonnull DataConverter converter) {
        return value;
    }

    protected abstract Schema.Type getAvroSchemaType();

    @JsonValue
    @Override
    public String toString() {
        String name = TypeCode.nameOf(typeCode);
        return nullable ? name + "|" + AbstractDingoType.NULLABLE : name;
    }
}
