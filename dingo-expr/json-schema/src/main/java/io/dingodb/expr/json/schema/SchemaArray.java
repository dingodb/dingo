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

package io.dingodb.expr.json.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.json.runtime.RtSchema;
import io.dingodb.expr.json.runtime.RtSchemaArray;
import io.dingodb.expr.json.runtime.RtSchemaLeaf;
import io.dingodb.expr.json.runtime.RtSchemaTuple;
import org.checkerframework.checker.nullness.qual.NonNull;

@JsonTypeName("array")
@JsonPropertyOrder({"items", "additionalItems"})
public final class SchemaArray extends Schema {
    @JsonProperty("items")
    private ArrayItems items;

    @JsonProperty("additionalItems")
    private Boolean additionalItems;

    @Override
    public @NonNull RtSchema createRtSchema() {
        if (additionalItems == null || additionalItems) {
            if (items == null) {
                return new RtSchemaLeaf(TypeCode.LIST);
            }
            SchemaType type = items.getType();
            switch (type) {
                case INTEGER:
                    return new RtSchemaArray(TypeCode.LONG);
                case NUMBER:
                    return new RtSchemaArray(TypeCode.DOUBLE);
                case STRING:
                    return new RtSchemaArray(TypeCode.STRING);
                case BOOLEAN:
                    return new RtSchemaArray(TypeCode.BOOL);
                default:
                    return new RtSchemaArray(TypeCode.OBJECT);
            }
        }
        Schema[] schemas = items.getSchemas();
        RtSchema[] children = new RtSchema[schemas.length];
        for (int i = 0; i < schemas.length; i++) {
            children[i] = schemas[i].createRtSchema();
        }
        return new RtSchemaTuple(children);
    }
}
