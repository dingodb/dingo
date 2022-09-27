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

import io.dingodb.common.type.NullType;
import io.dingodb.common.type.NullableType;
import io.dingodb.common.type.converter.DataConverter;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.serial.schema.DingoSchema;
import org.apache.avro.Schema;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class AbstractScalarType extends NullableType {
    protected AbstractScalarType(int typeCode, boolean nullable) {
        super(typeCode, nullable);
    }

    @Override
    protected Schema toAvroSchemaNotNull() {
        Schema.Type t = getAvroSchemaType();
        return Schema.create(t);
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
