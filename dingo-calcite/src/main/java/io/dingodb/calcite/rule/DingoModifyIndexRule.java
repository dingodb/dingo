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

import io.dingodb.calcite.rel.LogicalDingoTableScan;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.immutables.value.Value;

@Slf4j
@Value.Enclosing
public class DingoModifyIndexRule extends RelRule<RelRule.Config> implements SubstitutionRule {
    /**
     * Creates a temporary RelRule for update index col.
     *
     * @param config
     */
    protected DingoModifyIndexRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        //The update index column will not use indexes for now, and will be changed in the next version
        RelNode modify = call.rel(0);
        RelNode project = call.rel(1);
        RelNode input = project.getInput(0);
        if (input instanceof RelSubset) {
            RelSubset relSubset = (RelSubset) input;
            if (relSubset.getRelList().size() == 1) {
                RelNode input1 = relSubset.getRelList().get(0);
                if (input1 instanceof LogicalDingoTableScan) {
                    LogicalDingoTableScan scan = (LogicalDingoTableScan) input1;
                    scan.setForDml(true);
                }
            } else if (relSubset.getRelList().size() == 2) {
                RelNode input1 = relSubset.getRelList().get(1);
                if (input1 instanceof LogicalDingoTableScan) {
                    LogicalDingoTableScan scan = (LogicalDingoTableScan) input1;
                    scan.setForDml(true);
                }
            }
        }
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        DingoModifyIndexRule.Config DEFAULT = ImmutableDingoModifyIndexRule.Config.builder()
            .description("DingoModifyIndexRule")
            .operandSupplier(b0 ->
                b0.operand(LogicalTableModify.class).oneInput(b1 ->
                    b1.operand(LogicalProject.class).anyInputs()
                )
            )
            .build();

        @Override
        default DingoModifyIndexRule toRule() {
            return new DingoModifyIndexRule(this);
        }
    }
}
