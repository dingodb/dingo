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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DingoRelStreamingDef extends RelTraitDef<DingoRelStreaming> {
    public static final DingoRelStreamingDef INSTANCE = new DingoRelStreamingDef();

    private DingoRelStreamingDef() {
    }

    @Override
    public Class<DingoRelStreaming> getTraitClass() {
        return DingoRelStreaming.class;
    }

    @Override
    public String getSimpleName() {
        return "streaming";
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method only called when `IterativeRuleDriver` is used.
     */
    @Override
    public @Nullable RelNode convert(
        RelOptPlanner planner,
        @NonNull RelNode rel,
        @NonNull DingoRelStreaming toTrait,
        boolean allowInfiniteCostConverters
    ) {
        return null;
    }

    @Override
    public boolean canConvert(
        RelOptPlanner planner,
        DingoRelStreaming fromTrait,
        DingoRelStreaming toTrait
    ) {
        return true;
    }

    @Override
    public DingoRelStreaming getDefault() {
        return DingoRelStreaming.NONE;
    }
}
