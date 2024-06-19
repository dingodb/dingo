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
import io.dingodb.calcite.rel.dingo.DingoRelOp;
import io.dingodb.calcite.rel.dingo.IndexRangeScan;
import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.ProjectOp;
import io.dingodb.expr.rel.op.RelOpBuilder;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.IndexOpExpr;
import io.dingodb.expr.runtime.expr.Val;
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
import java.util.List;
import java.util.stream.Collectors;

@Value.Enclosing
public class IndexCompareMergeOpRule extends RelRule<RelRule.Config> {
    protected IndexCompareMergeOpRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        // in: aggr(count) -> project -> indexRangeScan
        // out: indexScanWithRelop (filter, project, aggr)
        DingoRelOp dingoRelOp1 = call.rel(0);
        DingoRelOp dingoRelOp2 = call.rel(1);
        IndexRangeScan indexRangeScan = call.rel(2);
        DingoTable dingoTable = indexRangeScan.getTable().unwrap(DingoTable.class);
        assert dingoTable != null;
        Table table = dingoTable.getTable();
        Table indexTable = indexRangeScan.getIndexTable();
        if (indexTable == null) {
            return;
        }
        boolean noLookup = false;
        RelOp relOp = null;
        if (dingoRelOp2.getRelOp() instanceof ProjectOp) {
            ProjectOp projectOp = (ProjectOp) dingoRelOp2.getRelOp();
            Expr[] exprs = projectOp.getProjects();
            List<Expr> exprList = Arrays.asList(exprs);
            noLookup = exprList.stream().allMatch(expr -> {
                if (!(expr instanceof IndexOpExpr)) {
                    return false;
                }
                IndexOpExpr indexOpExpr = (IndexOpExpr) expr;
                if (!(indexOpExpr.getOperand1() instanceof Val)) {
                    return false;
                }
                Val val1 = (Val) indexOpExpr.getOperand1();
                if (!(val1.getType() instanceof IntType)) {
                    return false;
                }
                int ix = (int) val1.getValue();
                Column column = table.getColumns().get(indexRangeScan.getSelection().get(ix));
                return indexTable.getColumns().contains(column);
            });
            if (!noLookup) {
                return;
            }
            Expr[] exprsReplace = exprList.stream().map(expr -> {
                IndexOpExpr expr1 = (IndexOpExpr) expr;
                Val val1 = (Val) expr1.getOperand1();
                int ix = (int) val1.getValue();
                Column column = table.getColumns().get(indexRangeScan.getSelection().get(ix));
                int indexIx = indexTable.getColumns().indexOf(column);
                RexInputRef rexInputRef = new RexInputRef(indexIx, indexRangeScan.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER));
                return RexConverter.convert(rexInputRef);
            }).toArray(Expr[]::new);
            relOp = RelOpBuilder.builder()
                .project(exprsReplace)
                .build();
        }

        if (indexRangeScan.isLookup() || !noLookup || relOp == null) {
            return;
        }

        RelOp filterRelOp = null;
        RexNode rexFilter = indexRangeScan.getFilter();
        List<Column> columnNames = indexTable.getColumns();
        List<Integer> indexSelectionList = columnNames.stream().map(table.columns::indexOf).collect(Collectors.toList());
        Mapping mapping = Mappings.target(indexSelectionList, table.getColumns().size());
        if (indexRangeScan.getFilter() != null) {
            rexFilter = RexUtil.apply(mapping, rexFilter);
            if (rexFilter != null) {
                Expr expr = RexConverter.convert(rexFilter);
                filterRelOp = RelOpBuilder.builder()
                    .filter(expr)
                    .build();
            }
        }

        RelOp op = RelOpBuilder.builder(filterRelOp).add(relOp).add(dingoRelOp1.getRelOp()).build();
        call.transformTo(
            new DingoIndexScanWithRelOp(
                indexRangeScan.getCluster(),
                dingoRelOp1.getTraitSet(),
                dingoRelOp1.getHints(),
                indexRangeScan.getTable(),
                dingoRelOp1.getRowType(),
                op,
                rexFilter,
                true,
                0,
                (IndexTable) indexTable,
                true
            )
        );
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        IndexCompareMergeOpRule.Config INDEX_COMPARE_MERGE2P = ImmutableIndexCompareMergeOpRule.Config.builder()
            .description("IndexCompareMergeOpRule(merge2Op)")
            .operandSupplier(b0 ->
                b0.operand(DingoRelOp.class).oneInput(b1 ->
                    b1.operand(DingoRelOp.class).oneInput(b2 ->
                        b2.operand(IndexRangeScan.class)
                            .predicate(indexRangeScan -> !indexRangeScan.isLookup()).anyInputs()
                    )
                )
            )
            .build();

        @Override
        default IndexCompareMergeOpRule toRule() {
            return new IndexCompareMergeOpRule(this);
        }

    }
}
