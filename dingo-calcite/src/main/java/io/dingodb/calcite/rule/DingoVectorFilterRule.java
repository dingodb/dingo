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
import io.dingodb.calcite.rel.LogicalDingoVector;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.immutables.value.Value;

@Value.Enclosing
public class DingoVectorFilterRule extends RelRule<DingoVectorFilterRule.Config> implements SubstitutionRule {

    public DingoVectorFilterRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalFilter filter = call.rel(0);
        final LogicalDingoVector vector = call.rel(1);
        call.transformTo(
            new LogicalDingoVector(
                vector.getCluster(),
                vector.getTraitSet(),
                vector.getCall(),
                vector.getTable(),
                vector.getOperands(),
                vector.getIndexTableId(),
                vector.getIndexTable(),
                vector.getSelection(),
                filter.getCondition(),
                filter.getHints()
            )
        );
    }

    @Override
    public boolean autoPruneOld() {
        return true;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        DingoVectorFilterRule.Config DEFAULT = ImmutableDingoVectorFilterRule.Config.builder()
            .operandSupplier(b0 ->
                b0.operand(LogicalFilter.class).oneInput(b1 ->
                    b1.operand(LogicalDingoVector.class)
                        .predicate(rel -> {
                            boolean isFullSelection = rel.getRealSelection() == null;
                            if (!isFullSelection) {
                                DingoTable dingoTable = rel.getTable().unwrap(DingoTable.class);
                                assert dingoTable != null;
                                isFullSelection = rel.getRealSelection().size()
                                    == dingoTable.getTable().getColumns().size();
                            }
                            return isFullSelection && rel.getFilter() == null;
                        } )
                        .noInputs()
                )
            )
            .description("DingoVectorFilterRule")
            .build();

        @Override
        default DingoVectorFilterRule toRule() {
            return new DingoVectorFilterRule(this);
        }
    }
}
