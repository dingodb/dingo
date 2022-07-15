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

import io.dingodb.common.type.DataConverter;
import io.dingodb.expr.runtime.TypeCode;
import org.apache.avro.Schema;

import java.sql.Time;
import javax.annotation.Nonnull;

public class TimeType extends AbstractScalarType {
    public TimeType(boolean nullable) {
        super(TypeCode.TIME, nullable);
    }

    @Override
    public TimeType copy() {
        return new TimeType(nullable);
    }

    @Override
    protected Object convertValueTo(@Nonnull Object value, @Nonnull DataConverter converter) {
        return converter.convert((Time) value);
    }

    @Override
    protected Object convertValueFrom(@Nonnull Object value, @Nonnull DataConverter converter) {
        return converter.convertTimeFrom(value);
    }

    @Override
    protected Schema.Type getAvroSchemaType() {
        return Schema.Type.LONG;
    }
}
