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

import io.dingodb.calcite.rel.DingoHashJoin;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

@Value.Enclosing
public class DingoHashJoinDistributeRule extends RelRule<DingoHashJoinDistributeRule.Config> {
    protected DingoHashJoinDistributeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        LogicalJoin rel = call.rel(0);
        JoinInfo joinInfo = rel.analyzeCondition();
        RelTraitSet traits = rel.getTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.of(joinInfo.leftKeys));
        call.transformTo(
            new DingoHashJoin(
                rel.getCluster(),
                traits,
                rel.getHints(),
                convert(rel.getLeft(), traits.replace((DingoRelStreaming.of(joinInfo.leftKeys)))),
                convert(rel.getRight(), traits.replace(DingoRelStreaming.of(joinInfo.rightKeys))),
                rel.getCondition(),
                rel.getVariablesSet(),
                rel.getJoinType()
            )
        );
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoHashJoinDistributeRule.Config.builder()
            .operandSupplier(b0 ->
                b0.operand(LogicalJoin.class).predicate(rel -> {
                    if (!DingoHashJoinRule.match(rel)) {
                        return false;
                    }
                    JoinInfo joinInfo = rel.analyzeCondition();
                    // No keys for redistribute, should be processed by `DingoHashJoinRule`.
                    return joinInfo.leftKeys.size() != 0 && joinInfo.rightKeys.size() != 0;
                }).anyInputs()
            )
            .description("DingoHashJoinDistributeRule")
            .build();

        @Override
        default DingoHashJoinDistributeRule toRule() {
            return new DingoHashJoinDistributeRule(this);
        }
    }
}
