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
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.type.converter.DataConverter;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.serial.schema.DingoSchema;
import org.apache.avro.Schema;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@JsonTypeName("null")
public class NullType extends AbstractDingoType {
    public static final NullType NULL = new NullType();

    private NullType() {
        super(TypeCode.NULL);
    }

    @JsonCreator
    public static NullType get() {
        return NULL;
    }

    @Override
    protected Object convertValueTo(@Nonnull Object value, @Nonnull DataConverter converter) {
        return null;
    }

    @Override
    protected Object convertValueFrom(@Nonnull Object value, @Nonnull DataConverter converter) {
        return null;
    }

    @Override
    public String toString() {
        return "NULL";
    }

    @Override
    public DingoType copy() {
        return this;
    }

    @Nonnull
    @Override
    public Schema toAvroSchema() {
        return Schema.create(Schema.Type.NULL);
    }

    @Override
    public List<DingoSchema> toDingoSchemas() {
        // TODO: no dingo null schema
        return null;
    }

    @Override
    public DingoSchema toDingoSchema(int index) {
        // TODO: no dingo null schema
        return null;
    }

    @Override
    public String format(@Nullable Object value) {
        return "NULL";
    }
}
