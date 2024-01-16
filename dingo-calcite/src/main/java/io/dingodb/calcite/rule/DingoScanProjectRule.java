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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Value.Enclosing
public class DingoScanProjectRule extends RelRule<DingoScanProjectRule.Config> implements SubstitutionRule {
    protected DingoScanProjectRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        final LogicalProject project = call.rel(0);
        final LogicalDingoTableScan scan = call.rel(1);
        final List<Integer> selectedColumns = new ArrayList<>();
        final RexVisitorImpl<Void> visitor = new RexVisitorImpl<Void>(true) {
            @Override
            public @Nullable Void visitInputRef(@NonNull RexInputRef inputRef) {
                if (!selectedColumns.contains(inputRef.getIndex())) {
                    selectedColumns.add(inputRef.getIndex());
                }
                return null;
            }
        };
        List<RexNode> projects = project.getProjects();
        RexNode filter = scan.getFilter();
        visitor.visitEach(projects);
        if (filter != null) {
            filter.accept(visitor);
        }
        // Order naturally to help decoding in push down.
        selectedColumns.sort(Comparator.naturalOrder());
        Mapping mapping = Mappings.target(selectedColumns, scan.getRowType().getFieldCount());
        // Push selection down over filter.
        RexNode newFilter = (filter != null) ? RexUtil.apply(mapping, filter) : null;
        LogicalDingoTableScan newScan = new LogicalDingoTableScan(
            scan.getCluster(),
            scan.getTraitSet(),
            scan.getHints(),
            scan.getTable(),
            newFilter,
            TupleMapping.of(selectedColumns),
            scan.getAggCalls(),
            scan.getGroupSet(),
            scan.getGroupSets(),
            scan.isPushDown(),
            scan.isForUpdate()
        );
        final List<RexNode> newProjectRexNodes = RexUtil.apply(mapping, project.getProjects());
        if (RexUtil.isIdentity(newProjectRexNodes, newScan.getRowType())) {
            call.transformTo(newScan);
        } else {
            call.transformTo(
                new LogicalProject(
                    project.getCluster(),
                    project.getTraitSet(),
                    project.getHints(),
                    newScan,
                    newProjectRexNodes,
                    project.getRowType(),
                    project.getVariablesSet()
                )
            );
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Left the old node to allow {@link org.apache.calcite.rel.rules.CoreRules#PROJECT_REMOVE} to work.
     */
    @Override
    public boolean autoPruneOld() {
        return false;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoScanProjectRule.Config.builder()
            .operandSupplier(b0 ->
                b0.operand(LogicalProject.class).oneInput(b1 ->
                    b1.operand(LogicalDingoTableScan.class)
                        .predicate(rel -> {
                            if (rel.getRealSelection() == null) {
                               return true;
                            }
                            DingoTable dingoTable = rel.getTable().unwrap(DingoTable.class);
                            return rel.getRealSelection().size() == dingoTable.getTable().getColumns().size();
                        } )
                        .noInputs()
                )
            )
            .description("DingoScanProjectRule")
            .build();

        @Override
        default DingoScanProjectRule toRule() {
            return new DingoScanProjectRule(this);
        }
    }
}
