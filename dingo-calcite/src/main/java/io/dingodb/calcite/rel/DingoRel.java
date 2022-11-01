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

import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.traits.DingoRelStreamingDef;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

public interface DingoRel extends PhysicalNode {
    static DingoRel dingo(RelNode rel) {
        return (DingoRel) rel;
    }

    <T> T accept(@NonNull DingoRelVisitor<T> visitor);

    default @NonNull DingoRelStreaming getStreaming() {
        return Objects.requireNonNull(this.getTraitSet().getTrait(DingoRelStreamingDef.INSTANCE));
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method is only called when `TopDownRuleDriver` is used.
     */
    @Override
    default @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(@NonNull RelTraitSet required) {
        return null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method is only called when `TopDownRuleDriver` is used.
     */
    @Override
    default @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        return null;
    }
}
