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
import io.dingodb.calcite.rel.dingo.DingoStreamingConverter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class DingoConvention extends Convention.Impl {
    public static Convention INSTANCE = new DingoConvention("DINGO", DingoRel.class);

    private DingoConvention(String name, Class<? extends RelNode> relClass) {
        super(name, relClass);
    }

    @Override
    public @Nullable RelNode enforce(@NonNull RelNode input, @NonNull RelTraitSet required) {
        assert DingoConvention.INSTANCE.equals(input.getConvention());
        assert DingoConvention.INSTANCE.equals(required.getConvention());
        DingoRelStreaming streaming = required.getTrait(DingoRelStreamingDef.INSTANCE);
        assert streaming != null;
        RelTraitSet targetTraits = input.getTraitSet().replace(streaming);
        if (targetTraits.satisfies(required)) {
            return new DingoStreamingConverter(input.getCluster(), targetTraits, input);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method is used only in `IterativeRuleDriver`, so we can convert traits by
     * {@link org.apache.calcite.plan.RelTraitDef#convert(RelOptPlanner, RelNode, RelTrait, boolean)}.
     */
    @Override
    public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits) {
        return true;
    }
}
