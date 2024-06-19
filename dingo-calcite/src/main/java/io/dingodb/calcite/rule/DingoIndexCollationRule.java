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
import io.dingodb.calcite.traits.DingoRelCollationImpl;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.ProjectOp;
import io.dingodb.expr.runtime.expr.IndexOpExpr;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.type.IntType;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.dingodb.calcite.rule.DingoFullScanProjectRule.getLogicalIndexScanWithRelOp;

@Value.Enclosing
public class DingoIndexCollationRule extends RelRule<DingoIndexCollationRule.Config> implements SubstitutionRule {
    protected DingoIndexCollationRule(DingoIndexCollationRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        config.matchHandler().accept(this, relOptRuleCall);
    }

    public static void removeCollationWithRelOp(DingoIndexCollationRule rule, RelOptRuleCall call) {
        LogicalSort logicalSort = call.rel(0);
        LogicalScanWithRelOp logicalScanWithRelOp = call.rel(1);

        if (RuleUtils.validateDisableIndex(logicalSort.getHints())) {
            return;
        }

        if (logicalSort.getCollation() instanceof DingoRelCollationImpl) {
            // order by primary cannot transform to indexScan
            DingoRelCollationImpl dingoRelCollation = (DingoRelCollationImpl) logicalSort.getCollation();
            if (dingoRelCollation.isMatchPrimary()) {
                return;
            }
        }
        RelOp relOp = logicalScanWithRelOp.getRelOp();
        if (!(relOp instanceof ProjectOp)) {
            return;
        }
        ProjectOp projectOp = (ProjectOp) relOp;
        if (!validateProjectOp(projectOp)) {
            return;
        }
        LogicalIndexScanWithRelOp indexScanWithRelOp = getLogicalIndexScanWithRelOp(logicalScanWithRelOp);
        if (indexScanWithRelOp == null) {
            return;
        }
        if (!"range".equalsIgnoreCase(indexScanWithRelOp.getIndexTable().getPartitionStrategy())) {
            return;
        }

        List<RelFieldCollation> relFieldCollationList = logicalSort.getCollation().getFieldCollations();
        Table table = logicalScanWithRelOp.getTable().unwrap(DingoTable.class).getTable();
        if (relFieldCollationList.size() != 1) {
            return;
        }
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
        int matchSortIx = indexScanWithRelOp.getIndexTable().getColumns().indexOf(orderCol);
        if (matchSortIx < 0) {
            return;
        }
        Column matchOrderIxCol = indexScanWithRelOp.getIndexTable().getColumns().get(matchSortIx);
        if (matchOrderIxCol.primaryKeyIndex != 0) {
            return;
        }
        int keepSerialOrder = RuleUtils.getSerialOrder(relFieldCollation);
        if (RuleUtils.preventRemoveOrder(keepSerialOrder)) {
            return;
        }
        RelCollation relCollation = RelCollationImpl.of(new ArrayList<>());
        indexScanWithRelOp.setKeepSerialOrder(keepSerialOrder);
        LogicalSort newSort = (LogicalSort) logicalSort.copy(
            logicalSort.getTraitSet(), indexScanWithRelOp, relCollation
        );
        call.transformTo(newSort);
    }

    public static int getIndexByExpr(IndexOpExpr indexOpExpr) {
        if (indexOpExpr.getOperand1() instanceof Val) {
            Val val1 = (Val) indexOpExpr.getOperand1();
            if (val1.getType() instanceof IntType) {
                return (int) val1.getValue();
            }
        }
        return -1;
    }

    public static boolean validateProjectOp(ProjectOp projectOp) {
        return Arrays.stream(projectOp.getProjects()).allMatch(expr -> expr instanceof IndexOpExpr);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {

        // select corp from tab order by age
        DingoIndexCollationRule.Config PROJECT_SORT = ImmutableDingoIndexCollationRule.Config.builder()
            .description("DingoIndexCollationRule(RemoveWithIndexRelOp)")
            .operandSupplier(b0 ->
                b0.operand(LogicalSort.class).oneInput(b1 ->
                        b1.operand(LogicalScanWithRelOp.class)
                            .noInputs()
                    )
            )
            .matchHandler(DingoIndexCollationRule::removeCollationWithRelOp)
            .build();

        @Override
        default DingoIndexCollationRule toRule() {
            return new DingoIndexCollationRule(this);
        }

        @Value.Parameter
        MatchHandler<DingoIndexCollationRule> matchHandler();
    }
}
