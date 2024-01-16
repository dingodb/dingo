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
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.entity.Table;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public final class RangeUtils {
    private RangeUtils() {
    }

    public static @Nullable RangeDistribution createRangeByFilter(
        Table table,
        KeyValueCodec codec,
        @NonNull RexNode sourceFilter,
        TupleMapping selection
    ) {
        if (sourceFilter.getKind() == SqlKind.NOT) {
            if (((RexCall) sourceFilter).operands.size() == 1) {
                return createRangeByFilter(table, codec, ((RexCall) sourceFilter).operands.get(0), selection);
            } else {
                return null;
            }
        }
        int firstPrimaryColumnIndex = table.keyMapping().get(0);
        int realIndex;
        if (selection != null) {
            realIndex = selection.find(firstPrimaryColumnIndex);
            if (realIndex < 0) {
                return null;
            }
        } else {
            realIndex = firstPrimaryColumnIndex;
        }
        byte[] start = null;
        byte[] end = null;
        boolean withStart = true;
        boolean withEnd = true;
        List<RexNode> filters = sourceFilter.getKind() == SqlKind.AND
            ? ((RexCall) sourceFilter).operands
            : Collections.singletonList(sourceFilter);
        byte[] conditionValue;
        for (RexNode filter : filters) {
            conditionValue = calcConditionValue(
                RuleUtils.checkCondition(filter), codec, realIndex, table.columns.size(), table
            );
            if (conditionValue == null) {
                return null;
            }
            int compare = 0;
            switch (filter.getKind()) {
                case LESS_THAN_OR_EQUAL: {
                    compare = 1;
                }
                case LESS_THAN: {
                    if (end == null || ByteArrayUtils.compare(conditionValue, end) <= compare) {
                        end = conditionValue;
                        withEnd = compare == 1;
                    }
                    break;
                }
                case GREATER_THAN_OR_EQUAL: {
                    compare = 1;
                }
                case GREATER_THAN: {
                    if (start == null || ByteArrayUtils.compare(conditionValue, start) >= compare) {
                        start = conditionValue;
                        withStart = compare == 1;
                    }
                    break;
                }
                default:
                    return null;
            }
        }
        return RangeDistribution.builder()
            .startKey(start)
            .endKey(end)
            .withStart(withStart)
            .withEnd(withEnd)
            .build();
    }

    private static byte[] calcConditionValue(RuleUtils.ConditionInfo info,
                                             KeyValueCodec codec,
                                             int pkIndex,
                                             int columnCount,
                                             Table tableDefinition) {
        if (info == null || info.index != pkIndex) {
            return null;
        }
        Object value = RexLiteralUtils.convertFromRexLiteral(
            info.value, tableDefinition.getColumns().get(pkIndex).getType()
        );
        if (value == null) {
            return null;
        }
        Object[] tuple = new Object[columnCount];
        tuple[pkIndex] = value;
        return codec.encodeKeyPrefix(tuple, 1);
    }
}
