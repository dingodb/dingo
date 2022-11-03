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

package io.dingodb.expr.core.exception;

import io.dingodb.expr.core.TypeCode;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.stream.Collectors;

public class IllegalParaTypes extends RuntimeException {
    private static final long serialVersionUID = -9010710020890934213L;

    @Getter
    protected final int[] paraTypeCodes;

    public IllegalParaTypes(String msgPrefix, int @Nullable [] paraTypeCodes) {
        super(msgPrefix + "parameter types " + getParaTypeNames(paraTypeCodes) + ".");
        this.paraTypeCodes = paraTypeCodes;
    }

    private static @NonNull String getParaTypeNames(int @Nullable [] paraTypeCodes) {
        if (paraTypeCodes != null) {
            return "(" + Arrays.stream(paraTypeCodes)
                .mapToObj(TypeCode::nameOf)
                .collect(Collectors.joining(", ")) + ")";
        }
        return "[Universal]";
    }
}
