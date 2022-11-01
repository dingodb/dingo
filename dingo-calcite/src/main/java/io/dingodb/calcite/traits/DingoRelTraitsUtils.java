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

package io.dingodb.calcite.traits;

import io.dingodb.calcite.rel.DingoRel;
import io.dingodb.calcite.rel.DingoStreamingConverter;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.stream.Collectors;

public final class DingoRelTraitsUtils {
    private DingoRelTraitsUtils() {
    }

    public static @NonNull RelNode convertStreaming(
        @NonNull RelNode rel,
        @NonNull DingoRelStreaming dstStreaming
    ) {
        return new DingoStreamingConverter(
            rel.getCluster(),
            rel.getTraitSet().replace(dstStreaming),
            rel
        );
    }

    @Nullable
    public static RelNode deriveToRelNode(@NonNull DingoRel rel, @NonNull RelTraitSet childTraits) {
        if (!childTraits.satisfies(rel.getTraitSet())) {
            final RelOptPlanner planner = rel.getCluster().getPlanner();
            return rel.copy(
                rel.getTraitSet(),
                // Inputs of derived `RelNode` are not registered after return and must be a `RelSubset`,
                // so register it here.
                rel.getInputs().stream()
                    .map(r -> RelOptRule.convert(r, childTraits))
                    .map(r -> convertStreaming(r, rel.getStreaming()))
                    .map(r -> planner.register(r, null))
                    .collect(Collectors.toList())
            );
        }
        return null;
    }
}
