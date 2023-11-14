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
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import io.dingodb.serial.schema.DingoSchema;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

@JsonTypeName("null")
public class NullType extends AbstractDingoType {
    public static final NullType NULL = new NullType();

    private NullType() {
        super();
    }

    @JsonCreator
    public static NullType get() {
        return NULL;
    }

    @Override
    protected Object convertValueTo(@NonNull Object value, @NonNull DataConverter converter) {
        return null;
    }

    @Override
    protected Object convertValueFrom(@NonNull Object value, @NonNull DataConverter converter) {
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

    @Override
    public Type getType() {
        return Types.NULL;
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
    public @NonNull String format(Object value) {
        return "NULL";
    }

    @Override
    public <R, T> R accept(@NonNull DingoTypeVisitor<R, T> visitor, T obj) {
        return visitor.visitNullType(this, obj);
    }
}
