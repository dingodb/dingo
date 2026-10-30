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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.OpKeys;
import io.dingodb.expr.runtime.op.VariadicOp;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * MySQL function {@code JSON_CONTAINS(target, candidate[, path])}.
 * <p>
 * Returns 1 if the candidate JSON document is contained in the target document, else 0.
 * Follows MySQL semantics:
 * <ul>
 *   <li>scalar vs scalar: contained if values are equal (numbers compared by value, e.g. 1 == 1.0)</li>
 *   <li>candidate array in target array: unordered subset match</li>
 *   <li>candidate array in target scalar: contained if any candidate element equals the target</li>
 *   <li>candidate object in target object: partial match, every candidate key must exist in target
 *       with a contained value</li>
 *   <li>object vs non-object: not contained</li>
 * </ul>
 * Returns {@code null} if any argument is {@code null} or the path does not exist in the target.
 */
public class JsonContainsFun extends VariadicOp {
    private static final long serialVersionUID = 120250419L;

    public static final JsonContainsFun INSTANCE = new JsonContainsFun();

    public static final String NAME = "JSON_CONTAINS";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Type getType() {
        return Types.INT;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    protected Object evalNonNullValue(@NonNull Object @NonNull [] values, ExprConfig config) {
        if (values.length < 2 || values.length > 3) {
            return null;
        }
        JsonNode target;
        JsonNode candidate;
        try {
            target = MAPPER.readTree(values[0].toString());
            candidate = MAPPER.readTree(values[1].toString());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON document in JSON_CONTAINS.", e);
        }
        if (values.length == 3) {
            JsonNode node = getPathNode(target, values[2].toString());
            if (node == null) {
                return null;
            }
            target = node;
        }
        return contains(target, candidate) ? 1 : 0;
    }

    /**
     * Resolves a simple path like {@code $}, {@code $.a}, {@code $.a[0].b} in the target document.
     * Returns {@code null} if the path does not exist.
     */
    private static @Nullable JsonNode getPathNode(@NonNull JsonNode target, @NonNull String path) {
        if (!path.startsWith("$")) {
            return null;
        }
        JsonNode node = target;
        String remainder = path.substring(1);
        // Split into segments of ".name" and "[index]".
        int i = 0;
        while (i < remainder.length() && node != null) {
            char c = remainder.charAt(i);
            if (c == '.') {
                int end = i + 1;
                while (end < remainder.length() && remainder.charAt(end) != '.' && remainder.charAt(end) != '[') {
                    end++;
                }
                String name = remainder.substring(i + 1, end);
                if (name.isEmpty()) {
                    return null;
                }
                node = node.get(name);
                i = end;
            } else if (c == '[') {
                int end = remainder.indexOf(']', i);
                if (end < 0) {
                    return null;
                }
                String indexStr = remainder.substring(i + 1, end);
                int index;
                try {
                    index = Integer.parseInt(indexStr);
                } catch (NumberFormatException e) {
                    return null;
                }
                node = node.get(index);
                i = end + 1;
            } else {
                return null;
            }
        }
        return node;
    }

    private static boolean contains(@NonNull JsonNode target, @NonNull JsonNode candidate) {
        if (target instanceof ArrayNode) {
            ArrayNode targetArray = (ArrayNode) target;
            if (candidate instanceof ArrayNode) {
                // Unordered subset: every candidate element must be contained in some target element.
                outer:
                for (JsonNode c : candidate) {
                    for (JsonNode t : targetArray) {
                        if (contains(t, c)) {
                            continue outer;
                        }
                    }
                    return false;
                }
                return true;
            }
            // Scalar or object candidate: contained if it matches any element of the target array.
            for (JsonNode t : targetArray) {
                if (contains(t, candidate)) {
                    return true;
                }
            }
            return false;
        }
        if (candidate instanceof ArrayNode) {
            // Candidate array in a non-array target is not contained, per MySQL rules.
            return false;
        }
        if (target.isObject() && candidate.isObject()) {
            // Partial match: every candidate key must exist in target with a contained value.
            var fields = candidate.fields();
            while (fields.hasNext()) {
                var entry = fields.next();
                JsonNode targetValue = target.get(entry.getKey());
                if (targetValue == null || !contains(targetValue, entry.getValue())) {
                    return false;
                }
            }
            return true;
        }
        if (target.isObject() != candidate.isObject()) {
            return false;
        }
        return equalsValue(target, candidate);
    }

    /**
     * Numbers must be compared by value across node types, e.g. {@code 1 == 1.0}.
     */
    private static boolean equalsValue(@NonNull JsonNode a, @NonNull JsonNode b) {
        if (a.isNumber() && b.isNumber()) {
            return a.decimalValue().compareTo(b.decimalValue()) == 0;
        }
        return a.equals(b);
    }

    @Override
    public OpKey keyOf(@NonNull Type @NonNull ... types) {
        return OpKeys.ALL_STRING.keyOf(types);
    }
}
