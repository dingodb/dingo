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
import io.dingodb.common.type.TupleMapping;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

public class DingoLikeScan extends LogicalDingoTableScan implements DingoRel {
    @Getter
    private final byte[] prefix;

    public DingoLikeScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        @Nullable RexNode filter,
        @Nullable TupleMapping selection,
        byte[] prefix
    ) {
        super(cluster, traitSet, hints, table, filter, selection);
        this.prefix = prefix;
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(@NonNull RelOptPlanner planner, @NonNull RelMetadataQuery mq) {
        // Assume that part scan has half cost.
        return Objects.requireNonNull(super.computeSelfCost(planner, mq)).multiplyBy(0.5d);
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
