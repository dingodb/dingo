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

package io.dingodb.calcite.rule.logical;

import io.dingodb.calcite.rel.logical.LogicalRelOp;
import io.dingodb.calcite.rel.logical.LogicalScanWithRelOp;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.RelOpBuilder;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

@Value.Enclosing
public class LogicalMergeRelOpScanRule extends RelRule<LogicalMergeRelOpScanRule.Config> implements SubstitutionRule {
    protected LogicalMergeRelOpScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        final LogicalRelOp rel = call.rel(0);
        final LogicalScanWithRelOp scan = call.rel(1);
        RelOp op = RelOpBuilder.builder(scan.getRelOp()).add(rel.getRelOp()).build();
        call.transformTo(
            new LogicalScanWithRelOp(
                scan.getCluster(),
                scan.getTraitSet(),
                scan.getHints(),
                scan.getTable(),
                rel.getRowType(),
                op,
                scan.isPushDown()
            )
        );
        call.getPlanner().prune(scan);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableLogicalMergeRelOpScanRule.Config.builder()
            .operandSupplier(b0 ->
                b0.operand(LogicalRelOp.class).oneInput(b1 ->
                    b1.operand(LogicalScanWithRelOp.class).noInputs()
                )
            )
            .description("LogicalMergeRelOpScanRule")
            .build();

        @Override
        default LogicalMergeRelOpScanRule toRule() {
            return new LogicalMergeRelOpScanRule(this);
        }
    }
}
