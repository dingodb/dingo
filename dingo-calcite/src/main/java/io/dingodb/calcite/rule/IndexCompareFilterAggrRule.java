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
import io.dingodb.calcite.rel.dingo.DingoIndexScanWithRelOp;
import io.dingodb.calcite.rel.dingo.DingoScanWithRelOp;
import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.expr.rel.CacheOp;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.FilterOp;
import io.dingodb.expr.rel.op.TandemPipeCacheOp;
import io.dingodb.expr.runtime.expr.BinaryOpExpr;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.IndexOpExpr;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.expr.VariadicOpExpr;
import io.dingodb.expr.runtime.op.logical.AndFun;
import io.dingodb.expr.runtime.type.IntType;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.dingodb.calcite.rule.DingoAggTransformRule.matchIndex;

@Value.Enclosing
public class IndexCompareFilterAggrRule extends RelRule<RelRule.Config> {
    protected IndexCompareFilterAggrRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        // index (age,corp)
        // select age,corp from tab where age>10
        // select age from tabb where age>10 and corp=11;
        DingoScanWithRelOp dingoScanWithRelOp = call.rel(0);
        if (RuleUtils.validateDisableIndex(dingoScanWithRelOp.getHints())) {
            return;
        }
        DingoTable dingoTable = dingoScanWithRelOp.getTable().unwrap(DingoTable.class);
        Table table = Objects.requireNonNull(dingoTable).getTable();
        if (!(dingoScanWithRelOp.getRelOp() instanceof TandemPipeCacheOp)){
            return;
        }
        TandemPipeCacheOp tandemPipeCacheOp = (TandemPipeCacheOp) dingoScanWithRelOp.getRelOp();
        FilterOp filterOp = (FilterOp) tandemPipeCacheOp.getInput();
        IndexTable indexTable;
        if (filterOp.getFilter() instanceof BinaryOpExpr) {
            BinaryOpExpr binaryOpExpr = (BinaryOpExpr) filterOp.getFilter();
            if (!(binaryOpExpr.getOperand0() instanceof IndexOpExpr)) {
                return;
            }
            IndexOpExpr indexOpExpr = (IndexOpExpr) binaryOpExpr.getOperand0();
            if (!(indexOpExpr.getOperand1() instanceof Val)) {
                return;
            }
            Val val1 = (Val) indexOpExpr.getOperand1();
            if (!(val1.getType() instanceof IntType)) {
                return;
            }
            int ix = (int) val1.getValue();
            Column column = table.getColumns().get(ix);
            indexTable = matchIndex(Collections.singletonList(column),
                dingoTable.getTable().getIndexes());
            if (indexTable == null) {
                return;
            }
            int indexIx = indexTable.getColumns().indexOf(column);
            Column indexCol = indexTable.getColumns().get(indexIx);
            boolean rangeScan = indexCol.getPrimaryKeyIndex() == 0;
            RexInputRef rexInputRef = new RexInputRef(indexIx,
                dingoScanWithRelOp.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER));
            Expr newIndexOpExpr = RexConverter.convert(rexInputRef);
            BinaryOpExpr newFilterOp
                = new BinaryOpExpr(binaryOpExpr.getOp(), newIndexOpExpr, binaryOpExpr.getOperand1());
            FilterOp filterOp1 = new FilterOp(newFilterOp);
            RelOp op = new TandemPipeCacheOp(filterOp1, (CacheOp) tandemPipeCacheOp.getOutput());

            RexNode rexFilter = dingoScanWithRelOp.getFilter();
            List<Column> columnNames = indexTable.getColumns();
            List<Integer> indexSelectionList = columnNames.stream()
                .map(dingoTable.getTable().columns::indexOf)
                .collect(Collectors.toList());
            Mapping mapping = Mappings.target(indexSelectionList,
                dingoTable.getTable().getColumns().size());
            if (rexFilter != null) {
                rexFilter = RexUtil.apply(mapping, rexFilter);
            }
            call.transformTo(
                new DingoIndexScanWithRelOp(
                    dingoScanWithRelOp.getCluster(),
                    dingoScanWithRelOp.getTraitSet(),
                    dingoScanWithRelOp.getHints(),
                    dingoScanWithRelOp.getTable(),
                    dingoScanWithRelOp.getRowType(),
                    op,
                    rexFilter,
                    true,
                    0,
                    indexTable,
                    rangeScan
                )
            );
        } else if (filterOp.getFilter() instanceof VariadicOpExpr) {
            VariadicOpExpr variadicOpExpr = (VariadicOpExpr) filterOp.getFilter();
            if (!(variadicOpExpr.getOp() instanceof AndFun)) {
                return;
            }
            List<Expr> exprList = Arrays.asList(variadicOpExpr.getOperands());
            boolean allFilter = exprList.stream().allMatch(expr -> {
                if (expr instanceof BinaryOpExpr) {
                    BinaryOpExpr binaryOpExpr = (BinaryOpExpr) expr;
                    return binaryOpExpr.getOperand0() instanceof IndexOpExpr;
                }
                return false;
            });
            if (!allFilter) {
                return;
            }
            List<Column> columnList = exprList.stream().map(expr -> {
                BinaryOpExpr binaryOpExpr = (BinaryOpExpr) expr;
                return binaryOpExpr.getOperand0();
            }).map(expr -> {
                IndexOpExpr indexOpExpr = (IndexOpExpr) expr;
                if (indexOpExpr.getOperand1() instanceof Val) {
                    Val val1 = (Val) indexOpExpr.getOperand1();
                    if (val1.getType() instanceof IntType) {
                        int ix = (int) val1.getValue();
                        return dingoTable.getTable().getColumns().get(ix);
                    }
                }
                return null;
            }).filter(Objects::nonNull).collect(Collectors.toList());
            indexTable = matchIndex(columnList, dingoTable.getTable().getIndexes());
            if (indexTable == null) {
                return;
            }
            AtomicBoolean rangeScan = new AtomicBoolean(false);
            Expr[] exprsReplace = exprList.stream().map(expr -> {
                BinaryOpExpr binaryOpExpr = (BinaryOpExpr) expr;
                IndexOpExpr expr1 = (IndexOpExpr) binaryOpExpr.getOperand0();
                Val val1 = (Val) expr1.getOperand1();
                int ix = (int) val1.getValue();
                Column column = dingoTable.getTable().getColumns().get(ix);
                int indexIx = indexTable.getColumns().indexOf(column);
                Column ixCol = indexTable.getColumns().get(indexIx);
                if (!rangeScan.get()) {
                    rangeScan.set(ixCol.primaryKeyIndex == 0);
                }
                RexInputRef rexInputRef = new RexInputRef(indexIx, dingoScanWithRelOp.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER));
                Expr indexOpExpr = RexConverter.convert(rexInputRef);
                return new BinaryOpExpr(binaryOpExpr.getOp(), indexOpExpr, binaryOpExpr.getOperand1());
            }).toArray(Expr[]::new);
            VariadicOpExpr variadicOpExpr1 = new VariadicOpExpr(variadicOpExpr.getOp(), exprsReplace);
            RelOp op = new TandemPipeCacheOp(new FilterOp(variadicOpExpr1), (CacheOp) tandemPipeCacheOp.getOutput());

            RexNode rexFilter = dingoScanWithRelOp.getFilter();
            List<Column> columnNames = indexTable.getColumns();
            List<Integer> indexSelectionList = columnNames.stream()
                .map(dingoTable.getTable().columns::indexOf)
                .collect(Collectors.toList());
            Mapping mapping = Mappings.target(indexSelectionList,
                dingoTable.getTable().getColumns().size());
            if (rexFilter != null) {
                rexFilter = RexUtil.apply(mapping, rexFilter);
            }

            call.transformTo(
                new DingoIndexScanWithRelOp(
                    dingoScanWithRelOp.getCluster(),
                    dingoScanWithRelOp.getTraitSet(),
                    dingoScanWithRelOp.getHints(),
                    dingoScanWithRelOp.getTable(),
                    dingoScanWithRelOp.getRowType(),
                    op,
                    rexFilter,
                    true,
                    0,
                    indexTable,
                    rangeScan.get()
                )
            );
        }
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config INDEX_COMPARE_FILTER_AGG_RULE = ImmutableIndexCompareFilterAggrRule.Config.builder()
            .description("IndexCompareFilterAggrRule")
            .operandSupplier(b0 ->
                b0.operand(DingoScanWithRelOp.class).predicate(dingoScanWithRelOp -> {
                    if (dingoScanWithRelOp.getRelOp() instanceof TandemPipeCacheOp) {
                        TandemPipeCacheOp pipeCacheOp = (TandemPipeCacheOp) dingoScanWithRelOp.getRelOp();
                        return pipeCacheOp.getInput() instanceof FilterOp;
                    }
                    return false;
                }).anyInputs()
            )
            .build();

        @Override
        default IndexCompareFilterAggrRule toRule() {
            return new IndexCompareFilterAggrRule(this);
        }
    }
}
