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

package io.dingodb.exec.fun.mysql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.dingodb.common.log.LogUtils;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.OpKeys;
import io.dingodb.expr.runtime.op.VariadicOp;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class JsonExtractFun extends VariadicOp {
    private static final long serialVersionUID = -8343792468386621027L;

    public static final JsonExtractFun INSTANCE = new JsonExtractFun();

    public static final String NAME = "JSON_EXTRACT";

    @Override
    public Type getType() {
        return Types.STRING;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public Object evalValue(Object @NonNull [] values, ExprConfig config) {
        if (values.length < 2) {
            return null;
        }
        Object value0 = values[0];
        if (value0 == null) {
            return null;
        }
        if (values.length == 2) {
            Object value1 = values[1];
            return getPathVal(value1, value0);
        } else {
            StringBuilder stringBuilder = new StringBuilder("[");
            for (int i = 1; i < values.length; i ++) {
                Object valueItem = values[i];
                Object val = getPathVal(valueItem, value0);
                if (val != null) {
                    stringBuilder.append(val).append(",");
                }
            }
            if (stringBuilder.length() > 1) {
                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            } else {
                return null;
            }
            return stringBuilder.append("]").toString();
        }
    }

    private static @Nullable Object getPathVal(Object value1, Object value0) {
        if (value1 == null) {
            return value0;
        }
        String path = value1.toString();
        String[] paths = path.split("\\.");
        if (!paths[0].startsWith("$")) {
            return null;
        }
        if (paths.length == 1 && paths[0].equals("$")) {
            return value0;
        }
        List<String> pathList = Arrays.asList(paths);
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode rootNode = mapper.readTree(value0.toString());
            JsonNode node = rootNode;
            List<JsonNode> jsonNodes;
            for (int i = 0; i < pathList.size(); i ++) {
                String pathItem = pathList.get(i);
                if (i == 0) {
                    if (pathItem.contains("[") && pathItem.contains("]")) {
                        int start = pathItem.indexOf("[");
                        int end = pathItem.indexOf("]");
                        if (end >= start + 1) {
                            String itemStr = pathItem.substring(start + 1, end);
                            if (isNumeric(itemStr)) {
                                int item = Integer.parseInt(itemStr);
                                node = rootNode.get(item);
                                if (node == null && !(rootNode instanceof ArrayNode) && item == 0) {
                                    node = rootNode;
                                }
                            }
                        } else {
                            return null;
                        }

                        if (pathItem.length() > (end + 1)) {
                            pathItem = pathItem.substring(end + 1);
                            if (pathItem.contains("[") && pathItem.contains("]")) {
                                start = pathItem.indexOf("[");
                                end = pathItem.indexOf("]");
                                if (node != null && end >= start + 1) {
                                    String itemStr = pathItem.substring(start + 1, end);
                                    if (isNumeric(itemStr)) {
                                        int item = Integer.parseInt(itemStr);
                                        pathItem = pathItem.substring(0, start);
                                        if ("".equalsIgnoreCase(pathItem)) {
                                            node = node.get(item);
                                        } else {
                                            node = node.get(pathItem).get(item);
                                        }
                                    } else if ("*".equalsIgnoreCase(itemStr)) {
                                        if (start > 0) {
                                            String fieldName = pathItem.substring(0, start);
                                            node = node.get(fieldName);
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        node = rootNode;
                    }
                } else {
                    if (pathItem.contains("[") && pathItem.contains("]")) {
                        int start = pathItem.indexOf("[");
                        int end = pathItem.indexOf("]");
                        if (node != null && end >= start + 1) {
                            String itemStr = pathItem.substring(start + 1, end);
                            if (isNumeric(itemStr)) {
                                int item = Integer.parseInt(itemStr);
                                pathItem = pathItem.substring(0, start);
                                node = node.get(pathItem).get(item);
                            } else if ("*".equalsIgnoreCase(itemStr)) {
                                if (start > 0) {
                                    String fieldName = pathItem.substring(0, start);
                                    node = node.get(fieldName);
                                }
                            }
                        }
                        // $[2][0]
                        if (pathItem.length() > (end + 1)) {
                            pathItem = pathItem.substring(end);
                            if (pathItem.contains("[") && pathItem.contains("]")) {
                                start = pathItem.indexOf("[");
                                end = pathItem.indexOf("]");
                                if (node != null && end >= start + 1) {
                                    String itemStr = pathItem.substring(start + 1, end);
                                    if (isNumeric(itemStr)) {
                                        int item = Integer.parseInt(itemStr);
                                        pathItem = pathItem.substring(0, start);
                                        node = node.get(pathItem).get(item);
                                    } else if ("*".equalsIgnoreCase(itemStr)) {
                                        if (start > 0) {
                                            String fieldName = pathItem.substring(0, start);
                                            node = node.get(fieldName);
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        if (node != null) {
                            if (node instanceof ArrayNode && !isNumeric(pathItem)) {
                                // $.name = null / $[*].name = ["", "", ""]
                                String prePathItem = pathList.get(i - 1);
                                boolean childAll = prePathItem.contains("[*]");
                                if (!childAll) {
                                    return null;
                                }

                                jsonNodes = rootNode.findValues(pathItem);
                                if (i == (pathList.size() - 1)) {
                                    if (jsonNodes.isEmpty()) {
                                        return null;
                                    }
                                    StringBuilder stringBuilder = new StringBuilder("[");
                                    jsonNodes.forEach(child -> {
                                        stringBuilder.append(child.toString()).append(",");
                                    });
                                    return stringBuilder.deleteCharAt(stringBuilder.length() - 1)
                                        .append("]").toString();
                                }
                            } else {
                                node = node.get(pathItem);
                            }
                        }
                    }
                }

            }
            if (node != null) {
                String res = node.toString();
                if (res.startsWith("\"") && res.endsWith("\"")) {
                    return res.substring(1, res.length() - 1);
                }
                return node.toString();
            }
            return null;
        } catch (JsonProcessingException e) {
            LogUtils.error(log, e.getMessage(), e);
            return null;
        } catch (Exception e) {
            LogUtils.error(log, "json extract failed,json:{}, path:{}", value0, value1);
            return null;
        }
    }

    public static boolean isNumeric(String str) {
        return str != null && str.matches("-?\\d+(\\.\\d+)?");
    }

    @Override
    public OpKey keyOf(@NonNull Type @NonNull ... types) {
        return OpKeys.ALL_STRING.keyOf(types);
    }
}
