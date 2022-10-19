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

import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.TypeCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor
public class RtSchemaLeaf extends RtSchema {
    private static final long serialVersionUID = -3661328042239398475L;
    @Getter
    private final int typeCode;
    @Getter
    private int index;

    @Override
    public int createIndex(int start) {
        index = start++;
        return start;
    }

    @Override
    public Object getId() {
        return index;
    }

    @Override
    public CompileContext getChild(Object index) {
        return null;
    }

    @Override
    public @NonNull String toString() {
        return TypeCode.nameOf(typeCode) + "(" + getIndex() + ")";
    }
}
