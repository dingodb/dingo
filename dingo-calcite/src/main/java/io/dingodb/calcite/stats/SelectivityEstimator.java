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

package io.dingodb.calcite.stats;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DingoRelOptTable;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.meta.entity.Column;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.Pair;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.IN;
import static org.apache.calcite.sql.SqlKind.IS_NOT_NULL;
import static org.apache.calcite.sql.SqlKind.IS_NULL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.LIKE;
import static org.apache.calcite.sql.SqlKind.NOT_EQUALS;

public class SelectivityEstimator extends RexVisitorImpl<Double> {

    public static final Set<SqlKind> COMPARISON =
        EnumSet.of(
            IN, EQUALS, NOT_EQUALS,
            LESS_THAN, GREATER_THAN,
            GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL,
            IS_NULL, IS_NOT_NULL, LIKE);
    private final RelNode childRel;
    private final RelMetadataQuery mq;

    public SelectivityEstimator(RelNode childRel, RelMetadataQuery mq) {
        super(true);
        this.childRel = childRel;
        this.mq = mq;
    }

    public Double estimateSelectivity(RexNode predicate) {
        return predicate.accept(this);
    }

    @Override
    public Double visitInputRef(RexInputRef inputRef) {
        if (inputRef.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
            return 0.5;
        }
        // If it is an embedded sub query, input ref is null
        return 0.1;
    }

    public Double visitCall(RexCall rexCall) {
        Double selectivity;
        if (rexCall.getKind() == SqlKind.AND) {
            selectivity = computeConjunctionSelectivity(rexCall);
        } else if (rexCall.isA(COMPARISON)) {
            selectivity = computeComparison(rexCall);
        } else if (rexCall.getKind() == SqlKind.OR) {
            selectivity = computeDisjunctionSelectivity(rexCall);
        } else if (rexCall.getKind() == SqlKind.NOT) {
            selectivity = computeNotSelectivity(rexCall);
        } else {
            selectivity = defaultSelectivity(rexCall);
        }
        return selectivity;
    }

    private double computeComparison(RexCall pred) {
        if (predicateMatch(pred) && childRel instanceof TableScan) {
            CalculateStatistic statistic = extractColStats(extractCol((TableScan) childRel, pred));
            if (statistic != null) {
                return statistic.estimateSelectivity(pred.getKind(),
                    extractVal(pred));
            }
        }
        return defaultSelectivity(pred);
    }

    private static Object extractVal(RexCall pred) {
        RexLiteral rexLiteral = (RexLiteral) pred.getOperands().get(1);
        return rexLiteral.getValue();
    }

    private static CalculateStatistic extractColStats(Pair<String, Column> statsIdentifier) {
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

    private static Pair<String, Column> extractCol(TableScan tableScan, RexCall pred) {
        RexInputRef rexInputRef = (RexInputRef) pred.getOperands().get(0);
        DingoTable dingoTable = tableScan.getTable().unwrap(DingoTable.class);
        String schemaName = dingoTable.getSchema().name();
        String tableName = schemaName + "." + dingoTable.getTable().getName();
        return Pair.of(tableName, dingoTable.getTable().getColumns().get(rexInputRef.getIndex()));
    }

    private boolean predicateMatch(RexCall pred) {
        ImmutableList operands = pred.operands;
        return operands.size() == 2
            && (operands.get(0) instanceof RexInputRef && operands.get(1) instanceof RexLiteral);
    }

    private Double computeConjunctionSelectivity(RexCall call) {
        Double tmpSelectivity;
        double selectivity = 1;

        for (RexNode cje : call.getOperands()) {
            tmpSelectivity = cje.accept(this);
            if (tmpSelectivity != null) {
                selectivity *= tmpSelectivity;
            }
        }

        return selectivity;
    }

    private Double computeDisjunctionSelectivity(RexCall pred) {
        // where amount = 1.3 or amount=3.3
        // where name in ('1','2','3','4')
        // all operands is RexCall and operatorKind = or
        if (pred.getOperands().stream().allMatch(rexNode -> rexNode instanceof RexCall)) {
            return computeOrSelectivity(pred, (LogicalDingoTableScan) childRel);
        } else {
            return defaultSelectivity(pred);
        }
    }

    private Double computeNotSelectivity(RexCall pred) {
        // where name not in ('1','2','3','4')
        // operatorKind = not and have one operand(kind = or)
        if (pred.getKind() == SqlKind.NOT && pred.getOperands().size() == 1
            && pred.getOperands().get(0) instanceof RexCall
            && pred.getOperands().get(0).getKind() == SqlKind.OR) {
            return computeNotOrSelectivity(pred, (LogicalDingoTableScan) childRel);
        } else {
            return defaultSelectivity(pred);
        }
    }

    public double computeNotOrSelectivity(RexNode rexNode, LogicalDingoTableScan tableScan) {
        RexCall pred = (RexCall) rexNode;
        return 1 - computeOrSelectivity(pred.getOperands().get(0), tableScan);
    }

    public double computeOrSelectivity(RexNode rexNode, LogicalDingoTableScan tableScan) {
        double sel = 1.0;
        double artificialSel = 0.0;
        List<RexNode> rexNodeList = RelOptUtil.disjunctions(rexNode);
        int predicateMatch = 0;
        for (RexNode pred : rexNodeList) {
            if (pred instanceof RexCall) {
                RexCall rexCall = (RexCall) pred;
                if (predicateMatch((RexCall) pred)) {
                    CalculateStatistic statistic = extractColStats(extractCol(tableScan, rexCall));
                    if (statistic != null) {
                        artificialSel += statistic.estimateSelectivity(pred.getKind(),
                            extractVal(rexCall));
                        predicateMatch ++;
                    }
                }
            }
        }
        if (predicateMatch < rexNodeList.size()) {
            artificialSel = defaultSelectivity(rexNode);
        }
        return sel * artificialSel;
    }

    private static double defaultSelectivity(RexNode pred) {
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
}
