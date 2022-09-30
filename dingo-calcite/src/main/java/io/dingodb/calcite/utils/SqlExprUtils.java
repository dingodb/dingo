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

import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.exec.expr.SqlExpr;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;

public final class SqlExprUtils {
    private SqlExprUtils() {
    }

    @Nonnull
    public static SqlExpr toSqlExpr(@Nonnull RexNode rexNode) {
        return toSqlExpr(rexNode, rexNode.getType());
    }

    @Nonnull
    public static SqlExpr toSqlExpr(@Nonnull RexNode rexNode, RelDataType type) {
        return new SqlExpr(
            RexConverter.convert(rexNode).toString(),
            DingoTypeFactory.fromRelDataType(type)
        );
    }

    @Nonnull
    public static List<SqlExpr> toSqlExprList(@Nonnull List<RexNode> rexNodes, RelDataType type) {
        return IntStream.range(0, rexNodes.size())
            .mapToObj(i -> toSqlExpr(rexNodes.get(i), type.getFieldList().get(i).getType()))
            .collect(Collectors.toList());
    }
}
