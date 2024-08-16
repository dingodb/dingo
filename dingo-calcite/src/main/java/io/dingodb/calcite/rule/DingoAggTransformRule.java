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

package io.dingodb.calcite.rule;

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.logical.LogicalIndexScanWithRelOp;
import io.dingodb.calcite.rel.logical.LogicalScanWithRelOp;
import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.expr.rel.CacheOp;
import io.dingodb.expr.rel.PipeOp;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.ProjectOp;
import io.dingodb.expr.rel.op.RelOpBuilder;
import io.dingodb.expr.rel.op.TandemPipeCacheOp;
import io.dingodb.expr.rel.op.UngroupedAggregateOp;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.IndexOpExpr;
import io.dingodb.expr.runtime.expr.NullaryAggExpr;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.op.aggregation.CountAllAgg;
import io.dingodb.expr.runtime.type.IntType;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import io.dingodb.meta.entity.Table;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.type.SqlTypeName;
import org.immutables.value.Value;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.dingodb.calcite.meta.DingoCostModelV1.getAvgRowSize;

@Value.Enclosing
public class DingoAggTransformRule extends RelRule<DingoAggTransformRule.Config> implements TransformationRule {
    public DingoAggTransformRule(DingoAggTransformRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        config.matchHandler().accept(this, relOptRuleCall);
    }

    public static void matchAggCount(DingoAggTransformRule rule, RelOptRuleCall call) {
        LogicalScanWithRelOp scan = call.rel(0);
        boolean disableIndex = !scan.getHints().isEmpty()
            && "disable_index".equalsIgnoreCase(scan.getHints().get(0).hintName);
        if (disableIndex) {
            return;
        }
        RelOp relOp = scan.getRelOp();
        if (relOp instanceof UngroupedAggregateOp) {
            UngroupedAggregateOp ugAgg = (UngroupedAggregateOp) relOp;
            List<Expr> aggList = ugAgg.getAggList();
            if (aggList != null && aggList.size() == 1) {
                boolean countAllAggMatch = aggList.stream().allMatch(expr -> {
                    if (expr instanceof NullaryAggExpr) {
                        NullaryAggExpr n = (NullaryAggExpr) expr;
                        return n.getOp() instanceof CountAllAgg;
                    }
                    return false;
                });
                if (countAllAggMatch) {
                    DingoTable dingoTable = scan.getTable().unwrap(DingoTable.class);
                    assert dingoTable != null;
                    List<IndexTable> indexTableList = dingoTable.getTable().getIndexes();
                    IndexTable indexTable = selMinCostIndex(indexTableList);
                    if (indexTable == null) {
                        return;
                    }
                    LogicalIndexScanWithRelOp indexScan
                        = new LogicalIndexScanWithRelOp(scan.getCluster(), scan.getTraitSet(), scan.getHints(),
                          scan.getTable(), scan.getRowType(),
                          relOp, null, true, 0, indexTable, false);
                    call.transformTo(indexScan);
                }
            }
        } else if (relOp instanceof TandemPipeCacheOp) {
            TandemPipeCacheOp pipeCacheOp = (TandemPipeCacheOp) relOp;
            RelOp inputRelOp = pipeCacheOp.getInput();
            DingoTable dingoTable = scan.getTable().unwrap(DingoTable.class);
            assert dingoTable != null;
            Table table = dingoTable.getTable();
            if (inputRelOp instanceof ProjectOp) {
                ProjectOp projectOp = (ProjectOp) inputRelOp;
                Expr[] exprs = projectOp.getProjects();
                List<Expr> exprList = Arrays.asList(exprs);
                List<Column> columnList = exprList.stream().map(expr -> {
                    if (expr instanceof IndexOpExpr) {
                        IndexOpExpr indexOpExpr = (IndexOpExpr) expr;
                        if (indexOpExpr.getOperand1() instanceof Val) {
                            Val val1 = (Val) indexOpExpr.getOperand1();
                            if (val1.getType() instanceof IntType) {
                                int ix = (int) val1.getValue();
                                return table.getColumns().get(ix);
                            }
                        }
                    }
                    return null;
                }).filter(Objects::nonNull).collect(Collectors.toList());
                IndexTable indexTable = matchIndex(columnList, dingoTable.getTable().getIndexes());
                if (indexTable == null) {
                    return;
                }

                Expr[] exprsReplace = exprList.stream().map(expr -> {
                    IndexOpExpr expr1 = (IndexOpExpr) expr;
                    Val val1 = (Val) expr1.getOperand1();
                    int ix = (int) val1.getValue();
                    Column column = table.getColumns().get(ix);
                    int indexIx = indexTable.getColumns().indexOf(column);
                    RexInputRef rexInputRef = new RexInputRef(indexIx, scan.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER));
                    return RexConverter.convert(rexInputRef);
                }).toArray(Expr[]::new);
                relOp = RelOpBuilder.builder()
                    .project(exprsReplace)
                    .build();
                pipeCacheOp = new TandemPipeCacheOp((PipeOp) relOp, (CacheOp) pipeCacheOp.getOutput());

                LogicalIndexScanWithRelOp indexScan
                    = new LogicalIndexScanWithRelOp(scan.getCluster(), scan.getTraitSet(), scan.getHints(),
                    scan.getTable(), scan.getRowType(),
                    pipeCacheOp, null, true, 0, indexTable, false);
                call.transformTo(indexScan);
            }
        }
    }

    public static IndexTable selMinCostIndex(List<IndexTable> indexTableList) {
        if (indexTableList == null || indexTableList.isEmpty()) {
            return null;
        }
        double cost = Integer.MAX_VALUE;
        int index = -1;
        for (int i = 0; i < indexTableList.size(); i ++) {
            IndexTable indexTable = indexTableList.get(i);
            if (indexTable.getSchemaState() != SchemaState.SCHEMA_PUBLIC) {
                continue;
            }
            if (indexTable.getIndexType() != IndexType.SCALAR) {
                continue;
            }
            double indexCost = getAvgRowSize(indexTable.getColumns(), indexTable, "");
            if (cost > indexCost) {
                cost = (int) indexCost;
                index = i;
            }
        }
        if (index == -1) {
            return null;
        }
        return indexTableList.get(index);
    }

    public static IndexTable matchIndex(List<Column> columnList, List<IndexTable> indexTableList) {
        return indexTableList.stream()
            .filter(indexTable -> indexTable.getSchemaState() == SchemaState.SCHEMA_PUBLIC
                && indexTable.getIndexType() == IndexType.SCALAR)
            .filter(indexTable -> indexTable.getColumns().containsAll(columnList))
            .findFirst().orElse(null);
    }

    @Value.Immutable()
    public interface Config extends RelRule.Config {
        DingoAggTransformRule.Config AGG_COUNT_TRANSFORM = ImmutableDingoAggTransformRule.Config.builder()
            .matchHandler(DingoAggTransformRule::matchAggCount)
            .build()
            .withOperandSupplier(b0 ->
                b0.operand(LogicalScanWithRelOp.class).predicate(scan -> {
                    RelOp relOp = scan.getRelOp();
                    return relOp instanceof UngroupedAggregateOp || relOp instanceof TandemPipeCacheOp;
                }).anyInputs())
            .withDescription("DingoAggTransformRule:aggCountTransform");

        @Override default DingoAggTransformRule toRule() {
            return new DingoAggTransformRule(this);
        }

        /** Forwards a call to {@link #onMatch(RelOptRuleCall)}. */
        MatchHandler<DingoAggTransformRule> matchHandler();

    }
}
