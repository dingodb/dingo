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

import io.dingodb.expr.runtime.TypeCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public final class RtSchemaDict extends RtSchema {
    private static final long serialVersionUID = -959587395199304595L;

    @Getter
    private final Map<String, RtSchema> children;

    @Override
    public int createIndex(int start) {
        for (RtSchema s : children.values()) {
            start = s.createIndex(start);
        }
        return start;
    }

    @Override
    public int getIndex() {
        return -1;
    }

    @Override
    public int getTypeCode() {
        return TypeCode.DICT;
    }

    @Override
    public RtSchema getChild(Object index) {
        if (index instanceof String) {
            return children.get(index);
        }
        throw new IllegalArgumentException("Index must be a string, but is \"" + index + "\"");
    }

    @Override
    public @NonNull String toString() {
        return "{" + children.entrySet().stream()
            .map(e -> e.getKey() + ": " + e.getValue())
            .collect(Collectors.joining(", ")) + "}";
    }
}
