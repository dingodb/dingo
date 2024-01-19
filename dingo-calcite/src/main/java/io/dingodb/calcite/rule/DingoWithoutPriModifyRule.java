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
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.entity.Column;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

@Value.Enclosing
public class DingoWithoutPriModifyRule extends RelRule<DingoWithoutPriModifyRule.Config> implements SubstitutionRule {

    /**
     * Creates a RelRule.
     *
     * @param config config
     */
    protected DingoWithoutPriModifyRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        config.matchHandler().accept(this, call);
    }

    public static void delete(DingoWithoutPriModifyRule rule, @NonNull RelOptRuleCall call) {
        LogicalTableModify modify = call.rel(0);
        LogicalDingoTableScan tableScan = call.rel(1);
        DingoTable dingoTable = tableScan.getTable().unwrap(DingoTable.class);
        assert dingoTable != null;
        List<Column> columns = dingoTable.getTable().getColumns();
        boolean withoutPri = false;
        for (Column columnDefinition : columns) {
            if (columnDefinition.isAutoIncrement() && columnDefinition.getState() == 2) {
                withoutPri = true;
                break;
            }
        }
        if (!withoutPri) {
            return;
        }
        TupleMapping originalSelection = tableScan.getSelection();
        if (originalSelection.contains(columns.size() - 1)) {
            return;
        }

        TupleMapping actualSelection = originalSelection.add(columns.size() - 1);
        LogicalDingoTableScan newScan = new LogicalDingoTableScan(
            tableScan.getCluster(),
            tableScan.getTraitSet(),
            tableScan.getHints(),
            tableScan.getTable(),
            tableScan.getFilter(),
            originalSelection,
            tableScan.getAggCalls(),
            tableScan.getGroupSet(),
            tableScan.getGroupSets(),
            tableScan.isPushDown(),
            tableScan.isForDml()
        );
        newScan.setSelectionForDml(actualSelection);
        List<RelNode> inputs = new ArrayList<>();
        inputs.add(newScan);
        LogicalTableModify newModify = modify.copy(modify.getTraitSet(), inputs);
        call.transformTo(newModify);
    }

    public static void update(DingoWithoutPriModifyRule rule, @NonNull RelOptRuleCall call) {
        LogicalTableModify modify = call.rel(0);
        LogicalProject project = call.rel(1);
        LogicalDingoTableScan tableScan = call.rel(2);
        DingoTable dingoTable = tableScan.getTable().unwrap(DingoTable.class);
        assert dingoTable != null;
        List<Column> columns = dingoTable.getTable().getColumns();
        boolean withoutPri = false;
        for (Column columnDefinition : columns) {
            if (columnDefinition.isAutoIncrement() && columnDefinition.getState() == 2) {
                withoutPri = true;
                break;
            }
        }
        if (!withoutPri) {
            return;
        }
        TupleMapping originalSelection = tableScan.getSelection();
        if (originalSelection.contains(columns.size() - 1)) {
            return;
        }

        TupleMapping actualSelection = originalSelection.add(columns.size() - 1);
        LogicalDingoTableScan newScan = new LogicalDingoTableScan(
            tableScan.getCluster(),
            tableScan.getTraitSet(),
            tableScan.getHints(),
            tableScan.getTable(),
            tableScan.getFilter(),
            originalSelection,
            tableScan.getAggCalls(),
            tableScan.getGroupSet(),
            tableScan.getGroupSets(),
            tableScan.isPushDown(),
            tableScan.isForDml()
        );
        newScan.setSelectionForDml(actualSelection);

        LogicalProject logicalProject = LogicalProject.create(newScan, project.getHints(),
            project.getProjects(), project.getRowType());

        List<RelNode> inputs = new ArrayList<>();
        inputs.add(logicalProject);
        LogicalTableModify newModify = modify.copy(modify.getTraitSet(), inputs);
        call.transformTo(newModify);
    }

    @Override
    public boolean autoPruneOld() {
        return true;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        DingoWithoutPriModifyRule.Config DELETE = ImmutableDingoWithoutPriModifyRule.Config.builder()
            .description("DingoWithoutPriModifyRule(delete)")
            .operandSupplier(b0 ->
                b0.operand(LogicalTableModify.class).oneInput(
                    b2 -> b2.operand(LogicalDingoTableScan.class).noInputs())
            ).matchHandler(DingoWithoutPriModifyRule::delete)
            .build();

        DingoWithoutPriModifyRule.Config UPDATE = ImmutableDingoWithoutPriModifyRule.Config.builder()
            .description("DingoWithoutPriModifyRule(update)")
            .operandSupplier(b0 ->
                b0.operand(LogicalTableModify.class).oneInput(
                    b1 -> b1.operand(LogicalProject.class)
                        .oneInput(b2 -> b2.operand(LogicalDingoTableScan.class).noInputs())
                )).matchHandler(DingoWithoutPriModifyRule::update)
            .build();

        @Override
        default DingoWithoutPriModifyRule toRule() {
            return new DingoWithoutPriModifyRule(this);
        }

        @Value.Parameter
        MatchHandler<DingoWithoutPriModifyRule> matchHandler();
    }
}
