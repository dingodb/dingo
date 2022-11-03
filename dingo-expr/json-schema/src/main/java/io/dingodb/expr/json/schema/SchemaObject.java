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
import io.dingodb.expr.json.runtime.RtSchemaDict;
import io.dingodb.expr.json.runtime.RtSchemaLeaf;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.Map;

@JsonTypeName("object")
@JsonPropertyOrder({"properties", "additionalProperties"})
public final class SchemaObject extends Schema {
    @JsonProperty("properties")
    @Getter
    private Map<String, Schema> properties;

    @JsonProperty("additionalProperties")
    @Getter
    private Boolean additionalProperties;

    @Override
    public @NonNull RtSchema createRtSchema() {
        if (additionalProperties == null || additionalProperties) {
            return new RtSchemaLeaf(TypeCode.MAP);
        }
        if (properties == null) {
            return new RtSchemaLeaf(TypeCode.OBJECT);
        }
        Map<String, RtSchema> children = new HashMap<>(properties.size());
        for (Map.Entry<String, Schema> entry : properties.entrySet()) {
            children.put(entry.getKey(), entry.getValue().createRtSchema());
        }
        return new RtSchemaDict(children);
    }
}
