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

import io.dingodb.calcite.rel.DingoTableScan;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.immutables.value.Value;

import javax.annotation.Nonnull;

@Value.Enclosing
public class DingoFilterScanRule extends RelRule<DingoFilterScanRule.Config> {
    protected DingoFilterScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@Nonnull RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final DingoTableScan scan = call.rel(1);
        call.transformTo(
            new DingoTableScan(
                scan.getCluster(),
                scan.getTraitSet(),
                scan.getHints(),
                scan.getTable(),
                filter.getCondition(),
                scan.getSelection()
            )
        );
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoFilterScanRule.Config.builder()
            .operandSupplier(b0 ->
                b0.operand(Filter.class).oneInput(b1 ->
                    b1.operand(DingoTableScan.class).predicate(rel -> rel.getFilter() == null).noInputs()
                )
            )
            .description("DingoFilterScanRule")
            .build();

        @Override
        default DingoFilterScanRule toRule() {
            return new DingoFilterScanRule(this);
        }
    }
}
