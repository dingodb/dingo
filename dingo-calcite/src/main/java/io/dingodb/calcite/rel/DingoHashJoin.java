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

package io.dingodb.calcite.rel;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.traits.DingoRelTraitsUtils;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

public class DingoHashJoin extends Join implements DingoRel {
    public DingoHashJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelNode left,
        RelNode right,
        RexNode condition,
        Set<CorrelationId> variablesSet,
        JoinRelType joinType
    ) {
        super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
    }

    @Override
    public Join copy(
        RelTraitSet traitSet,
        RexNode conditionExpr,
        RelNode left,
        RelNode right,
        JoinRelType joinType,
        boolean semiJoinDone
    ) {
        return new DingoHashJoin(
            getCluster(),
            traitSet,
            getHints(),
            left,
            right,
            conditionExpr,
            getVariablesSet(),
            joinType
        );
    }

    @Override
    public @Nullable RelNode derive(RelTraitSet childTraits, int childId) {
        RelTraitSet traits = getTraitSet();
        JoinInfo joinInfo = analyzeCondition();
        DingoRelStreaming leftStreaming;
        DingoRelStreaming rightStreaming;
        if (
            getStreaming().equals(DingoRelStreaming.ROOT)
                || joinInfo.leftKeys.size() == 0
                || joinInfo.rightKeys.size() == 0
        ) {
            leftStreaming = DingoRelStreaming.ROOT;
            rightStreaming = DingoRelStreaming.ROOT;
        } else {
            leftStreaming = DingoRelStreaming.of(joinInfo.leftKeys);
            rightStreaming = DingoRelStreaming.of(joinInfo.rightKeys);
        }
        if (childId == 0 && !childTraits.satisfies(traits.replace(leftStreaming))
            || childId == 1 && !childTraits.satisfies(traits.replace(rightStreaming))
        ) {
            RelOptPlanner planner = getCluster().getPlanner();
            RelNode left = DingoRelTraitsUtils.convertStreaming(getInput(0), leftStreaming);
            RelNode right = DingoRelTraitsUtils.convertStreaming(getInput(1), rightStreaming);
            return copy(
                traits,
                ImmutableList.of(
                    planner.register(left, null),
                    planner.register(right, null)
                )
            );
        }
        return null;
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
