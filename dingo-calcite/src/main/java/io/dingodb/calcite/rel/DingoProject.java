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
import io.dingodb.calcite.visitor.DingoRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public final class DingoProject extends Project implements DingoRel {
    public DingoProject(
        RelOptCluster cluster,
        RelTraitSet traits,
        List<RelHint> hints,
        RelNode input,
        List<? extends RexNode> projects,
        RelDataType rowType
    ) {
        super(cluster, traits, hints, input, projects, rowType);
    }

    @Override
    public @NonNull Project copy(
        RelTraitSet traitSet,
        RelNode input,
        List<RexNode> projects,
        RelDataType rowType
    ) {
        return new DingoProject(
            getCluster(),
            traitSet,
            getHints(),
            input,
            projects,
            rowType
        );
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public @NonNull Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        return Pair.of(childTraits, ImmutableList.of(childTraits));
    }

    @Override
    public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(@NonNull RelTraitSet required) {
        return Pair.of(required, ImmutableList.of(required));
    }
}
