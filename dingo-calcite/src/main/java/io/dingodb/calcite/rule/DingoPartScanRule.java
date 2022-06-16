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

import io.dingodb.calcite.DingoConventions;
import io.dingodb.calcite.rel.DingoPartScan;
import io.dingodb.calcite.rel.DingoTableScan;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.immutables.value.Value;

import javax.annotation.Nonnull;

@Value.Enclosing
public class DingoPartScanRule extends RelRule<DingoPartScanRule.Config> {
    protected DingoPartScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@Nonnull RelOptRuleCall call) {
        DingoTableScan rel = call.rel(0);
        call.transformTo(
            new DingoPartScan(
                rel.getCluster(),
                rel.getTraitSet().replace(DingoConventions.DISTRIBUTED),
                rel.getTable(),
                rel.getFilter(),
                rel.getSelection()
            )
        );
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoPartScanRule.Config.builder()
            .description("DingoPartScanRule")
            .operandSupplier(b0 ->
                b0.operand(DingoTableScan.class).trait(DingoConventions.DINGO).noInputs()
            )
            .build();

        @Override
        default DingoPartScanRule toRule() {
            return new DingoPartScanRule(this);
        }
    }
}
