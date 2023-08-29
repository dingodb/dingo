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

package io.dingodb.test.dsl.builder.checker;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class SqlResultCheckerDeserializer extends StdDeserializer<SqlResultChecker> {
    private static final long serialVersionUID = 8524495192713673075L;

    protected SqlResultCheckerDeserializer() {
        super(SqlResultChecker.class);
    }

    @Override
    public SqlResultChecker deserialize(
        @NonNull JsonParser parser,
        @NonNull DeserializationContext context
    ) throws IOException {
        JsonNode jsonNode = parser.readValueAsTree();
        if (jsonNode.isTextual()) {
            return new SqlCsvFileNameResultChecker(jsonNode.asText());
        } else if (jsonNode.isIntegralNumber()) {
            return new SqlResultCountChecker(jsonNode.asInt());
        } else if (jsonNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            List<String> lines = new LinkedList<>();
            boolean failed = false;
            for (JsonNode node : arrayNode) {
                if (node.isTextual()) {
                    lines.add(node.asText());
                } else {
                    failed = true;
                    break;
                }
            }
            if (!failed) {
                return new SqlCsvStringResultChecker(lines.toArray(new String[]{}));
            }
        }
        return (SqlResultChecker) context.handleUnexpectedToken(_valueClass, parser);
    }
}
