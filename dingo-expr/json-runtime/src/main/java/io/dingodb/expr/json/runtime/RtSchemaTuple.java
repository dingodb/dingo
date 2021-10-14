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

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

@RequiredArgsConstructor
public final class RtSchemaTuple extends RtSchema {
    private static final long serialVersionUID = -9026108762514887254L;

    @Getter
    private final RtSchema[] children;

    @Override
    public int createIndex(int start) {
        for (RtSchema s : children) {
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
        return TypeCode.TUPLE;
    }

    @Override
    public RtSchema getChild(Object index) {
        return children[(int) index];
    }

    @Nonnull
    @Override
    public String toString() {
        return "[" + Arrays.stream(children)
            .map(Objects::toString)
            .collect(Collectors.joining(", ")) + "]";
    }
}
