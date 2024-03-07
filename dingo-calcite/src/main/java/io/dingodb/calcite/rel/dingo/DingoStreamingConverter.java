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
import io.dingodb.calcite.traits.DingoRelPartition;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.traits.DingoRelStreamingDef;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

public final class DingoStreamingConverter extends SingleRel implements DingoRel {
    public DingoStreamingConverter(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input
    ) {
        super(cluster, traits, input);
    }

    @Override
    public double estimateRowCount(@NonNull RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(input);
        DingoRelStreaming inputStreaming = getInput().getTraitSet().getTrait(DingoRelStreamingDef.INSTANCE);
        assert inputStreaming != null;
        Set<DingoRelPartition> partitions = getStreaming().getPartitions();
        Set<DingoRelPartition> inputPartitions = inputStreaming.getPartitions();
        assert partitions != null && inputPartitions != null;
        if (partitions.size() > inputPartitions.size()) {
            for (int i = 0; i < partitions.size() - inputPartitions.size(); ++i) {
                rowCount /= 3.0d;
            }
        } else if (inputPartitions.size() > partitions.size()) {
            for (int i = 0; i < inputPartitions.size() - partitions.size(); ++i) {
                rowCount *= 3.0d;
            }
        }
        return rowCount;
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, @NonNull RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(input);
        DingoRelStreaming inputStreaming = getInput().getTraitSet().getTrait(DingoRelStreamingDef.INSTANCE);
        assert inputStreaming != null;
        if (getStreaming().getDistribution() != inputStreaming.getDistribution()) {
            return planner.getCostFactory().makeCost(rowCount, rowCount, rowCount);
        }
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DingoStreamingConverter(
            getCluster(),
            traitSet,
            sole(inputs)
        );
    }

    @Override
    public @Nullable RelNode passThrough(RelTraitSet required) {
        if (getInput().getTraitSet().satisfies(required)) {
            return getInput();
        }
        return copy(required, getInputs());
    }

    @Override
    public @Nullable RelNode derive(RelTraitSet childTraits, int childId) {
        RelNode newInput = RelOptRule.convert(getInput(), childTraits);
        return copy(getTraitSet(), ImmutableList.of(newInput));
    }
}
