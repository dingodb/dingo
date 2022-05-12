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
import io.dingodb.calcite.rel.DingoCoalesce;
import io.dingodb.calcite.rel.DingoExchange;
import io.dingodb.calcite.rel.DingoHash;
import io.dingodb.calcite.rel.DingoHashJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;

import java.util.List;
import javax.annotation.Nonnull;

public class DingoHashJoinRule extends RelRule<DingoHashJoinRule.Config> {
    protected DingoHashJoinRule(Config config) {
        super(config);
    }

    @Nonnull
    private static RelNode hashRedistribute(
        @Nonnull Join join,
        RelNode rel,
        List<Integer> keys
    ) {
        RelOptCluster cluster = join.getCluster();
        RelTraitSet traitSet = join.getTraitSet().replace(DingoConventions.DISTRIBUTED);
        return new DingoCoalesce(
            cluster,
            traitSet,
            new DingoExchange(
                cluster,
                traitSet,
                new DingoHash(
                    cluster,
                    traitSet,
                    convert(rel, DingoConventions.DISTRIBUTED),
                    keys
                )
            )
        );
    }

    @Override
    public void onMatch(@Nonnull RelOptRuleCall call) {
        Join rel = call.rel(0);
        JoinInfo joinInfo = rel.analyzeCondition();
        if (!joinInfo.isEqui()) {
            throw new RuntimeException("This type of join is not supported.");
        }
        RelOptCluster cluster = rel.getCluster();
        RelTraitSet traitSet = rel.getTraitSet().replace(DingoConventions.DISTRIBUTED);
        call.transformTo(
            new DingoHashJoin(
                cluster,
                traitSet,
                rel.getHints(),
                hashRedistribute(rel, rel.getLeft(), joinInfo.leftKeys),
                hashRedistribute(rel, rel.getRight(), joinInfo.rightKeys),
                rel.getCondition(),
                rel.getVariablesSet(),
                rel.getJoinType()
            )
        );
    }

    public interface Config extends RelRule.Config {
        Config DEFAULT = EMPTY
            .withOperandSupplier(b0 ->
                b0.operand(Join.class).trait(Convention.NONE).inputs(
                    b1 -> b1.operand(RelNode.class).anyInputs(),
                    b2 -> b2.operand(RelNode.class).anyInputs()
                )
            )
            .withDescription("DingoHashJoinRule")
            .as(Config.class);

        @Override
        default DingoHashJoinRule toRule() {
            return new DingoHashJoinRule(this);
        }
    }
}
