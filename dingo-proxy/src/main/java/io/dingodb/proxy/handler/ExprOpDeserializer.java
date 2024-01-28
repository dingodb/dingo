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
import io.dingodb.expr.runtime.op.OpType;

import java.io.IOException;

public class ExprOpDeserializer extends JsonDeserializer<OpType> {

    @Override
    public OpType deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException, JacksonException {
        String opType = jsonParser.getCodec().<JsonNode>readTree(jsonParser).asText().toUpperCase();
        switch (opType) {
            case "LT" : return OpType.LT;
            case "GT" : return OpType.GT;
            case "LTE" : return OpType.LE;
            case "GTE" : return OpType.GE;
            case "EQ" : return OpType.EQ;
            case "AND" : return OpType.AND;
            case "OR" : return OpType.OR;
            case "NOT": return OpType.NOT;
            default:
                throw new IllegalStateException("Unexpected value: " + opType);
        }
    }

}
