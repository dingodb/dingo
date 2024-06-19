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
import io.dingodb.calcite.rel.logical.LogicalScanWithRelOp;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.expr.rel.op.FilterOp;
import io.dingodb.expr.rel.op.ProjectOp;
import io.dingodb.expr.rel.op.TandemPipePipeOp;
import io.dingodb.expr.runtime.expr.IndexOpExpr;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexLiteral;
import org.immutables.value.Value;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.dingodb.calcite.rule.DingoIndexCollationRule.getIndexByExpr;
import static io.dingodb.calcite.rule.DingoIndexCollationRule.validateProjectOp;

@Value.Enclosing
public class DingoTableCollationRule extends RelRule<DingoTableCollationRule.Config> implements SubstitutionRule {
    protected DingoTableCollationRule(DingoTableCollationRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalSort logicalSort = call.rel(0);
        LogicalScanWithRelOp logicalScanWithRelOp = call.rel(1);

        if (RuleUtils.validateDisableIndex(logicalSort.getHints())) {
            return;
        }
        if (RuleUtils.matchTablePrimary(logicalSort)) {
            return;
        }
        if (logicalScanWithRelOp.getRelOp() instanceof TandemPipePipeOp) {
            logicalScanTandemSortRemove(call, logicalScanWithRelOp, logicalSort);
        } else if (logicalScanWithRelOp.getRelOp() instanceof FilterOp) {
            logicalScanFilterSortRemove(call, logicalSort, logicalScanWithRelOp);
        } else if (logicalScanWithRelOp.getRelOp() instanceof ProjectOp) {
            logicalScanProjectSortRemove(call, logicalScanWithRelOp, logicalSort);
        }
    }

    private static void logicalScanTandemSortRemove(
        RelOptRuleCall call,
        LogicalScanWithRelOp logicalScanWithRelOp,
        LogicalSort logicalSort
    ) {
        TandemPipePipeOp tandemPipePipeOp = (TandemPipePipeOp) logicalScanWithRelOp.getRelOp();
        if (!(tandemPipePipeOp.getOutput() instanceof ProjectOp && tandemPipePipeOp.getInput() instanceof FilterOp)) {
            return;
        }

        ProjectOp projectOp = (ProjectOp) tandemPipePipeOp.getOutput();
        if (!validateProjectOp(projectOp)) {
            return;
        }
        List<RelFieldCollation> relFieldCollationList = logicalSort.getCollation().getFieldCollations();
        if (relFieldCollationList.size() != 1) {
            return;
        }
        Table table = logicalScanWithRelOp.getTable().unwrap(DingoTable.class).getTable();
        Column orderCol = null;
        RelFieldCollation relFieldCollation = relFieldCollationList.get(0);
        int i = relFieldCollation.getFieldIndex();
        IndexOpExpr indexOpExpr = (IndexOpExpr) projectOp.getProjects()[i];
        int ix = getIndexByExpr(indexOpExpr);
        if (ix >= 0) {
            orderCol = table.getColumns().get(ix);
        }
        if (orderCol == null) {
            return;
        }
        if (orderCol.primaryKeyIndex != 0) {
            return;
        }
        int keepSerialOrder = RuleUtils.getSerialOrder(relFieldCollation);
        if (RuleUtils.preventRemoveOrder(keepSerialOrder)) {
            return;
        }
        RelCollation relCollation = RelCollationImpl.of(new ArrayList<>());
        logicalScanWithRelOp.setKeepSerialOrder(keepSerialOrder);
        LogicalSort logicalSort1 = (LogicalSort) logicalSort.copy(
            logicalSort.getTraitSet(), logicalScanWithRelOp, relCollation
        );
        call.transformTo(logicalSort1);
    }

    private static void logicalScanProjectSortRemove(RelOptRuleCall call, LogicalScanWithRelOp logicalScanWithRelOp, LogicalSort logicalSort) {
        ProjectOp projectOp = (ProjectOp) logicalScanWithRelOp.getRelOp();
        if (!validateProjectOp(projectOp)) {
            return;
        }
        List<RelFieldCollation> relFieldCollationList = logicalSort.getCollation().getFieldCollations();
        int limit = ScopeVariables.getRpcBatchSize();
        ;
        if (logicalSort.fetch instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) logicalSort.fetch;
            if (literal.getValue() instanceof BigDecimal) {
                limit = ((BigDecimal) literal.getValue()).intValue();
            }
        }
        if (relFieldCollationList.size() == 1) {
            removeSortByOrder(call, logicalScanWithRelOp, logicalSort, limit);
        } else if (relFieldCollationList.isEmpty() && logicalSort.fetch != null) {
            logicalScanWithRelOp.setKeepSerialOrder(1);
            logicalScanWithRelOp.setLimit(limit);
            LogicalSort logicalSort1 = (LogicalSort) logicalSort.copy(
                logicalSort.getTraitSet(), logicalScanWithRelOp, logicalSort.getCollation()
            );
            call.transformTo(logicalSort1);
        }
    }

    private static void removeSortByOrder(
        RelOptRuleCall call,
        LogicalScanWithRelOp logicalScanWithRelOp,
        LogicalSort logicalSort,
        int limit) {
        ProjectOp projectOp = (ProjectOp) logicalScanWithRelOp.getRelOp();

        List<RelFieldCollation> relFieldCollationList = logicalSort.getCollation().getFieldCollations();
        Table table = Objects.requireNonNull(logicalScanWithRelOp.getTable().unwrap(DingoTable.class)).getTable();
        RelFieldCollation relFieldCollation = relFieldCollationList.get(0);
        int i = relFieldCollation.getFieldIndex();
        IndexOpExpr indexOpExpr = (IndexOpExpr) projectOp.getProjects()[i];
        int ix = getIndexByExpr(indexOpExpr);
        Column orderCol = null;
        if (ix >= 0) {
            orderCol = table.getColumns().get(ix);
        }
        if (orderCol == null) {
            return;
        }
        if (orderCol.primaryKeyIndex != 0) {
            return;
        }
        int keepSerialOrder = RuleUtils.getSerialOrder(relFieldCollation);

        if (RuleUtils.preventRemoveOrder(keepSerialOrder)) {
            return;
        }
        RelCollation relCollation = RelCollationImpl.of(new ArrayList<>());
        logicalScanWithRelOp.setKeepSerialOrder(keepSerialOrder);
        logicalScanWithRelOp.setLimit(limit);
        LogicalSort logicalSort1 = (LogicalSort) logicalSort.copy(
            logicalSort.getTraitSet(), logicalScanWithRelOp, relCollation
        );
        call.transformTo(logicalSort1);
    }

    private static void logicalScanFilterSortRemove(RelOptRuleCall call, LogicalSort logicalSort, LogicalScanWithRelOp logicalScanWithRelOp) {
        List<RelFieldCollation> relFieldCollationList = logicalSort.getCollation().getFieldCollations();
        if (relFieldCollationList.size() != 1) {
            return;
        }
        Table table = Objects.requireNonNull(logicalScanWithRelOp.getTable().unwrap(DingoTable.class)).getTable();
        RelFieldCollation relFieldCollation = relFieldCollationList.get(0);
        int i = relFieldCollation.getFieldIndex();
        if (i < 0 || i >= table.getColumns().size()) {
            return;
        }
        Column orderCol = table.getColumns().get(i);
        if (orderCol.primaryKeyIndex != 0) {
            return;
        }

        int keepSerialOrder = RuleUtils.getSerialOrder(relFieldCollation);
        if (RuleUtils.preventRemoveOrder(keepSerialOrder)) {
            return;
        }
        RelCollation relCollation = RelCollationImpl.of(new ArrayList<>());
        logicalScanWithRelOp.setKeepSerialOrder(keepSerialOrder);
        LogicalSort logicalSort1 = (LogicalSort) logicalSort.copy(
            logicalSort.getTraitSet(), logicalScanWithRelOp, relCollation
        );
        call.transformTo(logicalSort1);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        DingoTableCollationRule.Config SORT_REMOVE_DINGO_SCAN = ImmutableDingoTableCollationRule.Config.builder()
            .description("DingoTableCollationRule(SORT_REMOVE_DINGO_SCAN)")
            .operandSupplier(b0 ->
                b0.operand(LogicalSort.class).oneInput(b1 ->
                    b1.operand(LogicalScanWithRelOp.class).predicate(scan -> {
                        DingoTable dingoTable = scan.getTable().unwrap(DingoTable.class);
                        assert dingoTable != null;
                        return "range".equalsIgnoreCase(dingoTable.getTable().getPartitionStrategy());
                    })
                        .noInputs()
                )
            )
            .build();

        @Override
        default DingoTableCollationRule toRule() {
            return new DingoTableCollationRule(this);
        }
    }
}
