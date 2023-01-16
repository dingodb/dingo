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
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.TupleMapping;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexLiteral;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class TableUtils {
    private TableUtils() {
    }

    public static TableDefinition getTableDefinition(@NonNull RelOptTable table) {
        return Objects.requireNonNull(table.unwrap(DingoTable.class)).getTableDefinition();
    }

    public static List<Object[]> getTuplesForMapping(
        @NonNull Collection<Map<Integer, RexLiteral>> items,
        @NonNull TableDefinition td,
        @NonNull TupleMapping mapping
    ) {
        final TupleMapping revMapping = mapping.reverse(td.getColumnsCount());
        return items.stream()
            .map(item -> {
                Object[] tuple = new Object[item.size()];
                for (Map.Entry<Integer, RexLiteral> entry : item.entrySet()) {
                    tuple[revMapping.get(entry.getKey())] = RexLiteralUtils.convertFromRexLiteral(
                        entry.getValue(),
                        td.getColumn(entry.getKey()).getType()
                    );
                }
                return tuple;
            })
            .collect(Collectors.toList());
    }

    public static List<Object[]> getTuplesForKeyMapping(
        @NonNull Collection<Map<Integer, RexLiteral>> items,
        @NonNull TableDefinition td
    ) {
        return getTuplesForMapping(items, td, td.getKeyMapping());
    }
}
