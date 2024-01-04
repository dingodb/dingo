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
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import io.dingodb.sdk.service.entity.common.ScalarField;
import io.dingodb.sdk.service.entity.common.ScalarField.DataNest.StringData;
import io.dingodb.sdk.service.entity.common.ScalarFieldType;
import io.dingodb.sdk.service.entity.common.ScalarValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ScalarValueDeserializer extends JsonDeserializer<ScalarValue> {
    @Override
    public ScalarValue deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext
    ) throws IOException, JacksonException {
        ObjectCodec codec = jsonParser.getCodec();
        JsonNode node = codec.readTree(jsonParser);
        ScalarFieldType scalarFieldType = ScalarFieldType.valueOf(node.get(ScalarValue.Fields.fieldType).asText());
        switch (scalarFieldType) {
            case STRING:
                List<ScalarField> fields = new ArrayList<>();
                for (JsonNode jsonNode : node.get("fields")) {
                    fields.add(ScalarField.builder().data(StringData.of(jsonNode.get("data").asText())).build());
                }
                return ScalarValue.builder()
                    .fieldType(scalarFieldType)
                    .fields(fields)
                    .build();
            case UNRECOGNIZED:
            case NONE:
            case BOOL:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case DOUBLE:
            case BYTES:
            default:
                throw new IllegalStateException("Unexpected value: " + scalarFieldType);
        }
    }
}
