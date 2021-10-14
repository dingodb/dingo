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

package io.dingodb.common.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.IOException;
import javax.annotation.Nonnull;

public class SqlTypeNameSerializer extends StdSerializer<SqlTypeName> {
    private static final long serialVersionUID = -3716419483226088191L;

    protected SqlTypeNameSerializer() {
        super(SqlTypeName.class);
    }

    @Override
    public void serialize(
        @Nonnull SqlTypeName type,
        @Nonnull JsonGenerator generator,
        SerializerProvider provider
    ) throws IOException {
        generator.writeString(type.getName().toLowerCase());
    }
}
