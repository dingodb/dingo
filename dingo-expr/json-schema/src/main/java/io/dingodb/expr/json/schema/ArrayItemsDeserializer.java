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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import javax.annotation.Nonnull;

final class ArrayItemsDeserializer extends StdDeserializer<ArrayItems> {
    private static final long serialVersionUID = 8668916270017428736L;

    ArrayItemsDeserializer() {
        super(ArrayItems.class);
    }

    @Override
    public ArrayItems deserialize(
        @Nonnull JsonParser parser,
        DeserializationContext ctx
    ) throws IOException {
        JsonNode jsonNode = parser.readValueAsTree();
        ArrayItems items = new ArrayItems();
        if (jsonNode instanceof ObjectNode) {
            ObjectNode objectNode = (ObjectNode) jsonNode;
            items.setType(parser.getCodec().treeToValue(objectNode.get("type"), SchemaType.class));
            return items;
        } else if (jsonNode instanceof ArrayNode) {
            items.setSchemas(parser.getCodec().treeToValue(jsonNode, Schema[].class));
            return items;
        }
        return (ArrayItems) ctx.handleUnexpectedToken(_valueClass, parser);
    }
}
