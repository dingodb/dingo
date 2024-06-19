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

import io.dingodb.calcite.rel.logical.LogicalScanWithRelOp;
import io.dingodb.calcite.traits.DingoRelCollationImpl;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.immutables.value.Value;

@Value.Enclosing
public class DingoRemoveSortRule extends RelRule<DingoRemoveSortRule.Config> implements SubstitutionRule {

    protected DingoRemoveSortRule(Config config) {
        super(config);
    }

    private static void matchPrimarySort(DingoRemoveSortRule rule, RelOptRuleCall call) {
        LogicalSort logicalSort = call.rel(0);
        LogicalScanWithRelOp scan = call.rel(1);
        boolean disableIndex = !logicalSort.getHints().isEmpty()
            && "disable_index".equalsIgnoreCase(logicalSort.getHints().get(0).hintName);
        if (disableIndex) {
            return;
        }
        if (logicalSort.getCollation() instanceof DingoRelCollationImpl) {
            DingoRelCollationImpl relCollation = (DingoRelCollationImpl) logicalSort.getCollation();
            scan.setKeepSerialOrder(relCollation.getOrder());
        }
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        config.matchHandler().accept(this, relOptRuleCall);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        DingoRemoveSortRule.Config REMOVE_PRIMARY_SORT = ImmutableDingoRemoveSortRule.Config.builder()
            .description("DingoRemoveSortRule(matchPrimarySort)")
            .operandSupplier(b0 ->
                b0.operand(LogicalSort.class).oneInput(b1 ->
                    b1.operand(LogicalScanWithRelOp.class)
                        .predicate(scan -> scan.getFilter() == null).noInputs()
                )
            )
            .matchHandler(DingoRemoveSortRule::matchPrimarySort)
            .build();

        @Override
        default DingoRemoveSortRule toRule() {
            return new DingoRemoveSortRule(this);
        }

        @Value.Parameter
        MatchHandler<DingoRemoveSortRule> matchHandler();
    }

}
