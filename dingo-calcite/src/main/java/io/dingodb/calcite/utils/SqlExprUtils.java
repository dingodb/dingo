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
import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.exec.expr.SqlExpr;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class SqlExprUtils {
    private SqlExprUtils() {
    }

    public static @NonNull SqlExpr toSqlExpr(RexNode rexNode) {
        return toSqlExpr(rexNode, rexNode.getType());
    }

    public static @NonNull SqlExpr toSqlExpr(RexNode rexNode, RelDataType type) {
        return new SqlExpr(
            RexConverter.convert(rexNode).toString(),
            DefinitionMapper.mapToDingoType(type)
        );
    }

    public static List<SqlExpr> toSqlExprList(@NonNull List<RexNode> rexNodes, RelDataType type) {
        return IntStream.range(0, rexNodes.size())
            .mapToObj(i -> toSqlExpr(rexNodes.get(i), type.getFieldList().get(i).getType()))
            .collect(Collectors.toList());
    }
}
