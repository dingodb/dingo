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

package io.dingodb.calcite.meta;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.stats.CalculateStatistic;
import io.dingodb.calcite.stats.StatsCache;
import io.dingodb.calcite.stats.TableStats;
import io.dingodb.common.table.ColumnDefinition;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.commons.lang3.tuple.Pair;

public class DingoRelMdSelectivity extends RelMdSelectivity {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.SELECTIVITY.method, new DingoRelMdSelectivity());

    public Double getSelectivity(LogicalDingoTableScan tableScan, RelMetadataQuery mq, RexNode predicate) {
        double sel = 1.0;
        if ((predicate == null) || predicate.isAlwaysTrue()) {
            return sel;
        }

        double artificialSel = 1.0;
        boolean useStats;
        for (RexNode pred : RelOptUtil.conjunctions(predicate)) {
            useStats = false;
            if (pred instanceof RexCall) {
                RexCall rexCall = (RexCall) pred;
                if (predicateMatch((RexCall) pred)) {
                    CalculateStatistic statistic = extractColStats(extractCol(tableScan, rexCall));
                    if (statistic != null) {
                        artificialSel *= statistic.estimateSelectivity(pred.getKind(),
                            extractVal(rexCall));
                        useStats = true;
                    }
                }
            }
            if (!useStats) {
                artificialSel *= defaultSelectivity(pred);
            }
        }
        return sel * artificialSel;
    }


    private double defaultSelectivity(RexNode pred) {
        if (pred.getKind() == SqlKind.IS_NOT_NULL) {
            return  0.9;
        } else if (
            (pred instanceof RexCall)
                && (((RexCall) pred).getOperator()
                == RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC)) {
            return RelMdUtil.getSelectivityValue(pred);
        } else if (pred.isA(SqlKind.EQUALS)) {
            return 0.15;
        } else if (pred.isA(SqlKind.COMPARISON)) {
            return 0.5;
        } else {
            return 0.25;
        }
    }

    private static boolean predicateMatch(RexCall pred) {
        ImmutableList operands = pred.operands;
        return operands.size() == 2 &&
            (operands.get(0) instanceof RexInputRef && operands.get(1) instanceof RexLiteral);
    }

    private Pair<String, ColumnDefinition> extractCol(LogicalDingoTableScan tableScan, RexCall pred) {
        RexInputRef rexInputRef = (RexInputRef) pred.getOperands().get(0);
        DingoTable dingoTable = (DingoTable) ((RelOptTableImpl) tableScan.getTable()).table();
        String schemaName = dingoTable.getSchema().name();
        String tableName = schemaName + "." + dingoTable.getTableDefinition().getName();
        return Pair.of(tableName, dingoTable.getTableDefinition().getColumn(rexInputRef.getIndex()));
    }

    private Object extractVal(RexCall pred) {
        RexLiteral rexLiteral = (RexLiteral) pred.getOperands().get(1);
        return rexLiteral.getValue();
    }

    private CalculateStatistic extractColStats(Pair<String, ColumnDefinition> statsIdentifier) {
        if (StatsCache.statsMap.containsKey(statsIdentifier.getLeft())) {
            TableStats stats = StatsCache.statsMap.get(statsIdentifier.getLeft());
            for (int i = 0; i < stats.getHistogramList().size(); i ++) {
                if (stats.getHistogramList().get(i).getColumnName().equals(statsIdentifier.getRight().getName())) {
                    return stats.getHistogramList().get(i);
                }
            }
            for (int i = 0; i < stats.getCountMinSketchList().size(); i ++) {
                if (stats.getCountMinSketchList().get(i).getColumnName().equals(statsIdentifier.getRight().getName())) {
                    return stats.getCountMinSketchList().get(i);
                }
            }
        }
        return null;
    }


}
