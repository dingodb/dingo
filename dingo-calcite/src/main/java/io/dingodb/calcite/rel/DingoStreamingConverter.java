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
import io.dingodb.calcite.traits.DingoRelStreamingDef;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

public class DingoStreamingConverter extends SingleRel implements DingoRel {
    public DingoStreamingConverter(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input
    ) {
        super(cluster, traits, input);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double rowCount = super.estimateRowCount(mq);
        DingoRelStreaming inputStreaming = getInput().getTraitSet().getTrait(DingoRelStreamingDef.INSTANCE);
        assert inputStreaming != null;
        if (getStreaming().getDistribution() != null && inputStreaming.getDistribution() == null) {
            return rowCount / 3.0d;
        }
        if (getStreaming().getDistribution() == null && inputStreaming.getDistribution() != null) {
            return rowCount * 3.0d;
        }
        return rowCount;
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public @NonNull Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        return Pair.of(getTraitSet(), ImmutableList.of(childTraits));
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DingoStreamingConverter(
            getCluster(),
            traitSet,
            sole(inputs)
        );
    }
}
