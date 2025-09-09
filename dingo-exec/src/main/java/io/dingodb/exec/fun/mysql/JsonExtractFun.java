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
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.List;

public class JsonExtractFun extends BinaryOp {
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
    public Object evalValue(Object value0, Object value1, ExprConfig config) {
        if (value0 == null) {
            return null;
        }
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
            for (int i = 0; i < pathList.size(); i ++) {
                String pathItem = pathList.get(i);
                if (i == 0) {
                    if (pathItem.contains("[") && pathItem.contains("]")) {
                        int start = pathItem.indexOf("[");
                        int end = pathItem.indexOf("]");
                        if (end >= start + 1) {
                            int item = Integer.parseInt(pathItem.substring(start + 1, end));
                            node = rootNode.get(item);
                            if (node == null && !(rootNode instanceof ArrayNode) && item == 0) {
                                node = rootNode;
                            }
                        } else {
                            return null;
                        }
                    } else {
                        node = rootNode;
                    }
                } else {
                    if (pathItem.contains("[") && pathItem.contains("]")) {
                        int start = pathItem.indexOf("[");
                        int end = pathItem.indexOf("]");
                        if (node != null && end >= start + 1) {
                            int item = Integer.parseInt(pathItem.substring(start + 1, end));
                            pathItem = pathItem.substring(0, start);
                            node = node.get(pathItem).get(item);
                        }
                    } else {
                        if (node != null) {
                            node = node.get(pathItem);
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
        } catch (Exception e) {
            return null;
        }
    }
}
