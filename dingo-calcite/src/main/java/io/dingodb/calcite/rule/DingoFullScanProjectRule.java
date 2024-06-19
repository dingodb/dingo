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
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.ProjectOp;
import io.dingodb.expr.rel.op.RelOpBuilder;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.IndexOpExpr;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import io.dingodb.meta.entity.Table;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.dingodb.calcite.rule.DingoAggTransformRule.selMinCostIndex;
import static io.dingodb.calcite.rule.DingoIndexCollationRule.getIndexByExpr;
import static io.dingodb.calcite.rule.DingoIndexCollationRule.validateProjectOp;

@Value.Enclosing
public class DingoFullScanProjectRule extends RelRule<RelRule.Config> {
    protected DingoFullScanProjectRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalScanWithRelOp scan = call.rel(0);
        if (RuleUtils.validateDisableIndex(scan.getHints()) || scan.getKeepSerialOrder() > 0) {
            return;
        }
        LogicalIndexScanWithRelOp logicalIndexScanWithRelOp = getLogicalIndexScanWithRelOp(scan);
        if (logicalIndexScanWithRelOp == null) return;
        call.transformTo(
            logicalIndexScanWithRelOp
        );
    }

    @Nullable
    public static LogicalIndexScanWithRelOp getLogicalIndexScanWithRelOp(LogicalScanWithRelOp scan) {
        Table table = Objects.requireNonNull(scan.getTable().unwrap(DingoTable.class)).getTable();

        ProjectOp projectOp = (ProjectOp) scan.getRelOp();
        if (!validateProjectOp(projectOp)) {
            return null;
        }
        Expr[] exprs = projectOp.getProjects();
        List<Expr> exprList = Arrays.asList(exprs);

        List<IndexTable> indexTableList = table.getIndexes();
        List<IndexTable> indexTableList1 = indexTableList.stream()
            .filter(indexTable -> indexTable.getIndexType() == IndexType.SCALAR)
            .filter(indexTable ->
                exprList.stream().allMatch(expr -> {
                    IndexOpExpr indexOpExpr = (IndexOpExpr) expr;
                    int ix = getIndexByExpr(indexOpExpr);
                    if (ix >= 0) {
                        Column column = table.getColumns().get(ix);
                        return indexTable.getColumns().contains(column);
                    }
                    return false;
                })).collect(Collectors.toList());
        if (indexTableList1.isEmpty()) {
            return null;
        }
        IndexTable indexTable = selMinCostIndex(indexTableList1);
        if (indexTable == null) {
            return null;
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
        RelOp relOp = RelOpBuilder.builder()
            .project(exprsReplace)
            .build();
        return new LogicalIndexScanWithRelOp(
            scan.getCluster(),
            scan.getTraitSet(),
            scan.getHints(),
            scan.getTable(),
            scan.getRowType(),
            relOp,
            scan.getFilter(),
            true,
            0,
            indexTable,
            false
        );
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        DingoFullScanProjectRule.Config DINGO_FULL_SCAN_PROJECT_RULE = ImmutableDingoFullScanProjectRule.Config.builder()
            .description("DingoFullScanProjectRule")
            .operandSupplier(b0 ->
                b0.operand(LogicalScanWithRelOp.class).trait(Convention.NONE)
                    .predicate(dingoScanWithRelOp -> dingoScanWithRelOp.getRelOp() instanceof ProjectOp).noInputs()
            )
            .build();

        @Override
        default DingoFullScanProjectRule toRule() {
            return new DingoFullScanProjectRule(this);
        }
    }
}
