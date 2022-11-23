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

package io.dingodb.calcite.utils;

import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.type.converter.RexLiteralConverter;
import io.dingodb.common.type.DingoType;
import org.apache.calcite.rex.RexLiteral;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

public final class RexLiteralUtils {
    private RexLiteralUtils() {
    }

    public static @Nullable Object convertFromRexLiteral(@NonNull RexLiteral rexLiteral, DingoType type) {
        if (!rexLiteral.isNull()) {
            // `rexLiteral.getType()` is not always the required type.
            return type.convertFrom(rexLiteral.getValue(), RexLiteralConverter.INSTANCE);
        }
        return null;
    }

    public static Object convertFromRexLiteral(@NonNull RexLiteral rexLiteral) {
        DingoType type = DefinitionMapper.mapToDingoType(rexLiteral.getType());
        return convertFromRexLiteral(rexLiteral, type);
    }

    public static Object @NonNull [] convertFromRexLiteralList(@NonNull List<RexLiteral> values, DingoType type) {
        return IntStream.range(0, values.size())
            .mapToObj(i -> convertFromRexLiteral(values.get(i), Objects.requireNonNull(type.getChild(i))))
            .toArray(Object[]::new);
    }
}
