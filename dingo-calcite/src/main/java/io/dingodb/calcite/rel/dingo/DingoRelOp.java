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

package io.dingodb.calcite.rel.dingo;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.rel.DingoRel;
import io.dingodb.calcite.rel.logical.LogicalRelOp;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import io.dingodb.expr.rel.RelOp;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public final class DingoRelOp extends LogicalRelOp implements DingoRel {
    public DingoRelOp(
        RelOptCluster cluster,
        RelTraitSet traits,
        List<RelHint> hints,
        RelNode input,
        RelDataType rowType,
        RelOp relOp
    ) {
        super(cluster, traits, hints, input, rowType, relOp);
    }

    @Override
    public @NonNull RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DingoRelOp(getCluster(), traitSet, hints, sole(inputs), rowType, relOp);
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visitDingoRelOp(this);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Return {@code null} to make the traits always consistent with its input, so no {@link DingoStreamingConverter}
     * is inserted in between. This is vital for SQL parameterized insert, where the parameterized value is implemented
     * as a {@code Project} with a dummy {@code Values} input. It is also important to push expr down.
     */
    @Override
    public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(@NonNull RelTraitSet required) {
        // return Pair.of(required, ImmutableList.of(required));
        return null;
    }

    @Override
    public @NonNull Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        return Pair.of(childTraits, ImmutableList.of(childTraits));
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        RelOptCost cost = super.computeSelfCost(planner, mq);
        assert cost != null;
        return cost.multiplyBy(0.8);
    }
}
