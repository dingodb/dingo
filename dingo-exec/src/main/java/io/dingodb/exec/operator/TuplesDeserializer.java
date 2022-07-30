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

package io.dingodb.exec.operator;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.JsonConverter;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nonnull;

final class TuplesDeserializer extends StdDeserializer<JsonNode> {
    private static final long serialVersionUID = -1878986711147886876L;

    TuplesDeserializer() {
        super(JsonNode.class);
    }

    @Nonnull
    static List<Object[]> convertBySchema(@Nonnull JsonNode jsonNode, DingoType schema) {
        if (jsonNode.isArray()) {
            List<Object[]> tuples = new LinkedList<>();
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            for (JsonNode node : arrayNode) {
                tuples.add((Object[]) schema.convertFrom(node, JsonConverter.INSTANCE));
            }
            return tuples;
        }
        throw new IllegalStateException("Tuples must be a json array.");
    }

    @Override
    public JsonNode deserialize(
        @Nonnull JsonParser parser,
        DeserializationContext ctx
    ) throws IOException {
        // Return raw `JsonNode` to parse with schema.
        return parser.readValueAsTree();
    }
}
