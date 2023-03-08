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

package io.dingodb.expr.json.runtime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import io.dingodb.expr.core.TypeCode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class DataParser extends Parser {
    private static final long serialVersionUID = -6849693677072717377L;

    private final RtSchemaRoot schemaRoot;

    private DataParser(DataFormat format, RtSchemaRoot schemaRoot) {
        super(format);
        this.schemaRoot = schemaRoot;
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.disable(SerializationFeature.INDENT_OUTPUT);
    }

    /**
     * Create a DataParser of json format.
     *
     * @return a DataParser
     */
    public static @NonNull DataParser json(RtSchemaRoot schemaRoot) {
        return new DataParser(DataFormat.APPLICATION_JSON, schemaRoot);
    }

    /**
     * Create a DataParser of yaml format.
     *
     * @return a new DataParser
     */
    public static @NonNull DataParser yaml(RtSchemaRoot schemaRoot) {
        return new DataParser(DataFormat.APPLICATION_YAML, schemaRoot);
    }

    /**
     * Create a DataParser of a specified format.
     *
     * @param format the DataFormat
     * @return a new DataParser
     */
    public static @NonNull DataParser get(DataFormat format, RtSchemaRoot schemaRoot) {
        return new DataParser(format, schemaRoot);
    }

    private static @Nullable Object jsonNodeValue(@NonNull JsonNode jsonNode) {
        JsonNodeType type = jsonNode.getNodeType();
        switch (type) {
            case NUMBER:
                if (jsonNode.isInt() || jsonNode.isLong()) {
                    return jsonNode.asLong();
                }
                return jsonNode.asDouble();
            case STRING:
                return jsonNode.asText();
            case BOOLEAN:
                return jsonNode.asBoolean();
            case ARRAY:
                List<Object> list = new LinkedList<>();
                for (int i = 0; i < jsonNode.size(); i++) {
                    list.add(jsonNodeValue(jsonNode.get(i)));
                }
                return list;
            case OBJECT:
                Map<String, Object> map = new HashMap<>(jsonNode.size());
                Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> entry = it.next();
                    map.put(entry.getKey(), jsonNodeValue(entry.getValue()));
                }
                return map;
            case NULL:
                return null;
            default:
                break;
        }
        throw new IllegalArgumentException("Unsupported json node type \"" + type + "\".");
    }

    private static void parseAccordingSchema(
        Object[] tuple,
        @NonNull JsonNode jsonNode,
        @NonNull RtSchema rtSchema
    ) {
        if (jsonNode.isNull()) {
            tuple[rtSchema.getIndex()] = null;
            return;
        }
        switch (rtSchema.getTypeCode()) {
            case TypeCode.TUPLE:
                RtSchemaTuple schemaTuple = (RtSchemaTuple) rtSchema;
                for (int i = 0; i < schemaTuple.getChildren().length; i++) {
                    JsonNode item = jsonNode.get(i);
                    if (item != null) {
                        parseAccordingSchema(tuple, jsonNode.get(i), schemaTuple.getChild(i));
                    }
                }
                return;
            case TypeCode.DICT:
                RtSchemaDict schemaDict = (RtSchemaDict) rtSchema;
                for (Map.Entry<String, RtSchema> entry : schemaDict.getChildren().entrySet()) {
                    String key = entry.getKey();
                    JsonNode child = jsonNode.get(key);
                    if (child != null) {
                        parseAccordingSchema(tuple, jsonNode.get(key), entry.getValue());
                    }
                }
                return;
            case TypeCode.INT:
                tuple[rtSchema.getIndex()] = jsonNode.asInt();
                return;
            case TypeCode.LONG:
                tuple[rtSchema.getIndex()] = jsonNode.asLong();
                return;
            case TypeCode.FLOAT:
                //TODO:
                tuple[rtSchema.getIndex()] = jsonNode.asDouble();
                return;
            case TypeCode.DOUBLE:
                tuple[rtSchema.getIndex()] = jsonNode.asDouble();
                return;
            case TypeCode.STRING:
                tuple[rtSchema.getIndex()] = jsonNode.asText();
                return;
            case TypeCode.BOOL:
                tuple[rtSchema.getIndex()] = jsonNode.asBoolean();
                return;
            case TypeCode.DECIMAL:
                tuple[rtSchema.getIndex()] = jsonNode.decimalValue();
                return;
            case TypeCode.ARRAY:
                int elementType = ((RtSchemaArray) rtSchema).getElementTypeCode();
                switch (elementType) {
                    case TypeCode.INT:
                        Integer[] integerArray = new Integer[jsonNode.size()];
                        for (int i = 0; i < jsonNode.size(); i++) {
                            integerArray[i] = jsonNode.get(i).asInt();
                        }
                        tuple[rtSchema.getIndex()] = integerArray;
                        return;
                    case TypeCode.LONG:
                        Long[] longArray = new Long[jsonNode.size()];
                        for (int i = 0; i < jsonNode.size(); i++) {
                            longArray[i] = jsonNode.get(i).asLong();
                        }
                        tuple[rtSchema.getIndex()] = longArray;
                        return;
                    case TypeCode.DOUBLE:
                        Double[] doubleArray = new Double[jsonNode.size()];
                        for (int i = 0; i < jsonNode.size(); i++) {
                            doubleArray[i] = jsonNode.get(i).asDouble();
                        }
                        tuple[rtSchema.getIndex()] = doubleArray;
                        return;
                    case TypeCode.BOOL:
                        Boolean[] booleanArray = new Boolean[jsonNode.size()];
                        for (int i = 0; i < jsonNode.size(); i++) {
                            booleanArray[i] = jsonNode.get(i).asBoolean();
                        }
                        tuple[rtSchema.getIndex()] = booleanArray;
                        return;
                    case TypeCode.STRING:
                    default:
                        String[] stringArray = new String[jsonNode.size()];
                        for (int i = 0; i < jsonNode.size(); i++) {
                            stringArray[i] = jsonNode.get(i).asText();
                        }
                        tuple[rtSchema.getIndex()] = stringArray;
                        return;
                }
            case TypeCode.LIST:
                if (jsonNode.isArray()) {
                    tuple[rtSchema.getIndex()] = jsonNodeValue(jsonNode);
                    return;
                }
                break;
            case TypeCode.MAP:
                if (jsonNode.isObject()) {
                    tuple[rtSchema.getIndex()] = jsonNodeValue(jsonNode);
                    return;
                }
                break;
            default:
                break;
        }
    }

    private static Object toListMapAccordingSchema(Object[] tuple, @NonNull RtSchema rtSchema) {
        int typeCode = rtSchema.getTypeCode();
        if (typeCode == TypeCode.TUPLE) {
            List<Object> list = new LinkedList<>();
            RtSchemaTuple schemaTuple = (RtSchemaTuple) rtSchema;
            for (int i = 0; i < schemaTuple.getChildren().length; i++) {
                list.add(toListMapAccordingSchema(tuple, schemaTuple.getChild(i)));
            }
            return list;
        } else if (typeCode == TypeCode.DICT) {
            Map<String, Object> map = new LinkedHashMap<>();
            RtSchemaDict schemaDict = (RtSchemaDict) rtSchema;
            for (Map.Entry<String, RtSchema> entry : schemaDict.getChildren().entrySet()) {
                map.put(entry.getKey(), toListMapAccordingSchema(tuple, entry.getValue()));
            }
            return map;
        } else {
            return tuple[rtSchema.getIndex()];
        }
    }

    /**
     * Parse a given String into a tuple.
     *
     * @param text the given String
     * @return the tuple
     * @throws JsonProcessingException if something is wrong
     */
    public Object @NonNull [] parse(String text) throws JsonProcessingException {
        JsonNode jsonNode = mapper.readTree(text);
        return jsonNodeToTuple(jsonNode);
    }

    /**
     * Read from a given InputStream and parse the contents into a tuple.
     *
     * @param is the given InputStream
     * @return the tuple
     * @throws IOException if something is wrong
     */
    public Object @NonNull [] parse(InputStream is) throws IOException {
        JsonNode jsonNode = mapper.readTree(new InputStreamReader(is));
        return jsonNodeToTuple(jsonNode);
    }

    /**
     * Serialize a tuple into a String.
     *
     * @param tuple the tuple
     * @return the serialized String
     * @throws JsonProcessingException if something is wrong
     */
    public String serialize(Object[] tuple) throws JsonProcessingException {
        Object object = toListMapAccordingSchema(tuple, schemaRoot.getSchema());
        return mapper.writeValueAsString(object);
    }

    private Object @NonNull [] jsonNodeToTuple(JsonNode jsonNode) {
        Object[] tuple = new Object[schemaRoot.getMaxIndex()];
        parseAccordingSchema(tuple, jsonNode, schemaRoot.getSchema());
        return tuple;
    }
}
