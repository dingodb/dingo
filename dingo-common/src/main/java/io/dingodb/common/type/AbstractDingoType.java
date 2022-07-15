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
import io.dingodb.common.type.converter.CsvConverter;
import io.dingodb.expr.runtime.TypeCode;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@EqualsAndHashCode(of = {"typeCode"})
public abstract class AbstractDingoType implements DingoType {
    public static final String NULLABLE = "NULL";

    @Getter
    protected final int typeCode;
    @Getter
    @Setter
    private Integer id;

    protected AbstractDingoType(int typeCode) {
        this.typeCode = typeCode;
    }

    @Nonnull
    @JsonCreator
    public static AbstractDingoType scalar(@Nonnull String typeString) {
        String[] v = typeString.split("\\|", 2);
        boolean nullable = v.length > 1 && v[1].equals(AbstractDingoType.NULLABLE);
        return DingoTypeFactory.scalar(TypeCode.codeOf(v[0]), nullable);
    }

    @Nonnull
    @JsonCreator
    public static AbstractDingoType tuple(String[] typeStrings) {
        return DingoTypeFactory.tuple(typeStrings);
    }

    protected abstract Object convertValueTo(@Nonnull Object value, @Nonnull DataConverter converter);

    protected abstract Object convertValueFrom(@Nonnull Object value, @Nonnull DataConverter converter);

    @Override
    public Object convertTo(@Nullable Object value, @Nonnull DataConverter converter) {
        if (value != null) {
            return convertValueTo(value, converter);
        }
        return null;
    }

    @Override
    public Object convertFrom(@Nullable Object value, @Nonnull DataConverter converter) {
        if (value != null && !converter.isNull(value)) {
            return convertValueFrom(value, converter);
        }
        return null;
    }

    @Override
    public Object parse(@Nullable Object value) {
        return convertFrom(value, CsvConverter.INSTANCE);
    }
}
