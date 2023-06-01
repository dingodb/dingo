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

import io.dingodb.calcite.visitor.DingoRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public class DingoUnion extends Union implements DingoRel {
    public DingoUnion(
        RelOptCluster cluster,
        RelTraitSet traits,
        List<RelNode> inputs,
        boolean all
    ) {
        super(cluster, traits, inputs, all);
    }

    @Override
    public DingoUnion copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new DingoUnion(getCluster(), traitSet, inputs, all);
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        int size = this.inputs.size();
        List<RelTraitSet> traitSetList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            traitSetList.add(childTraits);
        }
        return Pair.of(childTraits, traitSetList);
    }
}
