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
import io.dingodb.common.type.converter.DataConverter;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.serial.schema.BytesSchema;
import io.dingodb.serial.schema.DingoSchema;
import org.apache.avro.Schema;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

@JsonTypeName("decimal")
public class DecimalType extends AbstractScalarType {
    @JsonCreator
    public DecimalType(@JsonProperty("nullable") boolean nullable) {
        super(TypeCode.DECIMAL, nullable);
    }

    @Override
    public DecimalType copy() {
        return new DecimalType(nullable);
    }

    @Override
    public DingoSchema toDingoSchema(int index) {
        return new BytesSchema(index);
    }

    @Override
    protected Object convertValueTo(@Nonnull Object value, @Nonnull DataConverter converter) {
        return converter.convert((BigDecimal) value);
    }

    @Override
    protected Object convertValueFrom(@Nonnull Object value, @Nonnull DataConverter converter) {
        return converter.convertDecimalFrom(value);
    }

    @Override
    public Schema.Type getAvroSchemaType() {
        return Schema.Type.BYTES;
    }
}
