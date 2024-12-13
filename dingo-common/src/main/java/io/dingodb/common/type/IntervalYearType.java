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
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.serial.schema.DingoSchema;
import io.dingodb.serial.schema.StringSchema;
import lombok.EqualsAndHashCode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

@JsonTypeName("interval_year")
@JsonPropertyOrder({"element", "type", "nullable"})
@EqualsAndHashCode(of = {"element"}, callSuper = true)
public class IntervalYearType extends NullableType {

    @JsonProperty("type")
    private final Type type;
    @JsonProperty("element")
    private final Type element;

    @JsonCreator
    public IntervalYearType(
        @JsonProperty("nullable") boolean nullable,
        @JsonProperty("element") Type element) {
        super(nullable);
        this.element = element;
        this.type = Types.INTERVAL_YEAR;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public List<DingoSchema> toDingoSchemas() {
        return null;
    }

    @Override
    public @NonNull String format(@Nullable Object value) {
        return NullType.NULL.format(null);
    }

    @Override
    public DingoType copy() {
        return new IntervalYearType(nullable, element);
    }

    @Override
    public DingoSchema toDingoSchema(int index) {
        return new StringSchema(index, 0);
    }

    @Override
    public <R, T> R accept(@NonNull DingoTypeVisitor<R, T> visitor, T obj) {
        return visitor.visitIntervalYearType(this, obj);
    }

    @Override
    protected @Nullable Object convertValueTo(@NonNull Object value, @NonNull DataConverter converter) {
        return converter.convert(value);
    }

    @Override
    protected Object convertValueFrom(@NonNull Object value, @NonNull DataConverter converter) {
        return converter.convertIntervalFrom(value, type, element);
    }
}
