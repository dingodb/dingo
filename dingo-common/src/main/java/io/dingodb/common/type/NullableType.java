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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import org.apache.avro.Schema;
import org.checkerframework.checker.nullness.qual.NonNull;

@EqualsAndHashCode(of = {"nullable"}, callSuper = true)
public abstract class NullableType extends AbstractDingoType {
    protected final boolean nullable;

    protected NullableType(int typeCode, boolean nullable) {
        super(typeCode);
        this.nullable = nullable;
    }

    @Override
    public @NonNull Schema toAvroSchema() {
        Schema schema = toAvroSchemaNotNull();
        if (nullable) {
            // Allow avro to encode `null`.
            return Schema.createUnion(schema, Schema.create(Schema.Type.NULL));
        }
        return schema;
    }

    protected abstract Schema toAvroSchemaNotNull();

    // Need this getter to enable jackson afterburner module access.
    @JsonProperty(value = "nullable", defaultValue = "false")
    @JsonInclude(value = JsonInclude.Include.NON_DEFAULT)
    public boolean isNullable() {
        return nullable;
    }
}
