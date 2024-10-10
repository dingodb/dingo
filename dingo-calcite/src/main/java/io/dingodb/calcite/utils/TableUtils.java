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

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class TableUtils {
    private TableUtils() {
    }

    public static CommonId getTableId(@NonNull RelOptTable table) {
        return Objects.requireNonNull(table.unwrap(DingoTable.class)).getTableId();
    }

    public static List<Object[]> getTuplesForKeyMapping(
        @NonNull Collection<Map<Integer, RexNode>> items,
        @NonNull Table td
    ) {
        return items.stream()
            .map(item -> {
                Object[] tuple = new Object[td.getColumns().size()];
                for (Map.Entry<Integer, RexNode> entry : item.entrySet()) {
                    Expr expr = RexConverter.convert(entry.getValue());
                    Object val = ExprCompiler.ADVANCED.visit(expr).eval();
                    Column column = td.getColumns().get(entry.getKey());
                    if (column.getType() instanceof FloatType && val instanceof Double) {
                        tuple[entry.getKey()] = Float.valueOf(val.toString());
                    } else {
                        tuple[entry.getKey()] = val;
                    }
                }
                return tuple;
            })
            .collect(Collectors.toList());
    }

}
