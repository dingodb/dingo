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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.type.DataConverter;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.serial.schema.DingoSchema;
import io.dingodb.serial.schema.IntegerSchema;
import org.apache.avro.Schema;

import javax.annotation.Nonnull;

@JsonTypeName("int")
public class IntegerType extends AbstractScalarType {
    @JsonCreator
    public IntegerType(@JsonProperty("nullable") boolean nullable) {
        super(TypeCode.INT, nullable);
    }

    @Override
    public IntegerType copy() {
        return new IntegerType(nullable);
    }

    @Override
    public DingoSchema toDingoSchema(int index) {
        return new IntegerSchema(index);
    }

    @Override
    protected Object convertValueFrom(@Nonnull Object value, @Nonnull DataConverter converter) {
        return converter.convertIntegerFrom(value);
    }

    @Override
    protected Schema.Type getAvroSchemaType() {
        return Schema.Type.INT;
    }
}
