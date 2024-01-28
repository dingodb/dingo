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

package io.dingodb.proxy.handler;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import io.dingodb.sdk.service.entity.common.ScalarFieldType;

import java.io.IOException;

public class ExprTypeDeserializer extends JsonDeserializer<Type> {
    @Override
    public Type deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException, JacksonException {
        ScalarFieldType scalarFieldType = ScalarFieldType.valueOf(
            jsonParser.getCodec().<JsonNode>readTree(jsonParser).asText().toUpperCase()
        );
        switch (scalarFieldType) {
            case STRING: return Types.STRING;
            case BOOL: return Types.BOOL;
            case INT8:
            case INT16:
            case INT32: return Types.INT;
            case INT64: return Types.LONG;
            case FLOAT32: return Types.FLOAT;
            case DOUBLE: return Types.DOUBLE;
            case BYTES:
            case UNRECOGNIZED:
            case NONE:
            default:
                throw new IllegalStateException("Unexpected type: " + scalarFieldType);
        }
    }
}
