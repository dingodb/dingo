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
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * To reduce the results of partitioned aggregation.
 */
public final class DingoReduce extends SingleRel implements DingoRel {
    @Getter
    private final ImmutableBitSet groupSet;
    @Getter
    private final List<AggregateCall> aggregateCallList;
    @Getter
    private final RelDataType originalInputType;

    public DingoReduce(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        ImmutableBitSet groupSet,
        List<AggregateCall> aggregateCallList,
        RelDataType originalInputType
    ) {
        super(cluster, traits, input);
        this.input = input;
        this.groupSet = groupSet;
        this.aggregateCallList = aggregateCallList;
        this.originalInputType = originalInputType;
    }

    @Override
    public @NonNull RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DingoReduce(
            getCluster(),
            traitSet,
            sole(inputs),
            groupSet,
            aggregateCallList,
            originalInputType
        );
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(@NonNull RelOptPlanner planner, RelMetadataQuery mq) {
        // Assume that all reduces are needed.
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public double estimateRowCount(@NonNull RelMetadataQuery mq) {
        final int groupCount = groupSet.cardinality();
        if (groupCount == 0) {
            return 1;
        } else {
            double rowCount = super.estimateRowCount(mq);
            rowCount *= 1.0 - Math.pow(.5, groupCount);
            return rowCount;
        }
    }

    @Override
    public @NonNull RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("groupSet", groupSet);
        pw.item("aggregateCallList", aggregateCallList);
        pw.item("originalInputType", originalInputType);
        return pw;
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
