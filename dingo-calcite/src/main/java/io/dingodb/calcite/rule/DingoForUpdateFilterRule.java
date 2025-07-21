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
import io.dingodb.calcite.rel.LogicalForUpdate;
import io.dingodb.calcite.visitor.RexConverter;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.immutables.value.Value;

@Value.Enclosing
public class DingoForUpdateFilterRule extends RelRule<DingoForUpdateFilterRule.Config> implements SubstitutionRule {
    protected DingoForUpdateFilterRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalFilter filter = call.rel(0);
        LogicalForUpdate forUpdate = call.rel(1);
        LogicalDingoTableScan scan = call.rel(2);
        try {
            RexConverter.convert(filter.getCondition());
        } catch (RexConverter.UnsupportedRexNode e) {
            return;
        }

        call.transformTo(new LogicalForUpdate(
            forUpdate.getCluster(),
            forUpdate.getTraitSet(),
            new LogicalDingoTableScan(
                scan.getCluster(),
                scan.getTraitSet(),
                filter.getHints(),
                scan.getTable(),
                filter.getCondition(),
                scan.getRealSelection(),
                scan.getAggCalls(),
                scan.getGroupSet(),
                scan.getGroupSets(),
                scan.isPushDown(),
                scan.isForDml()
            ),
            forUpdate.getTable()
        ));

        call.getPlanner().prune(scan);
        call.getPlanner().prune(forUpdate);
    }

    @Override
    public boolean autoPruneOld() {
        return true;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoForUpdateFilterRule.Config.builder()
            .operandSupplier(b0 ->
                b0.operand(LogicalFilter.class).oneInput(b1 ->
                    b1.operand(LogicalForUpdate.class)
                        .oneInput(b2 ->
                            b2.operand(LogicalDingoTableScan.class).noInputs())
                ))
            .description("DingoForUpdateFilterRule")
            .build();


        @Override
        default DingoForUpdateFilterRule toRule() {
            return new DingoForUpdateFilterRule(this);
        }
    }
}
