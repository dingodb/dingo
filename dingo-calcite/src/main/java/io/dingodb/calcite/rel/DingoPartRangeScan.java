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
import io.dingodb.common.type.TupleMapping;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

public class DingoPartRangeScan extends LogicalDingoTableScan implements DingoRel {
    @Getter
    private final byte[] startKey;
    @Getter
    private final byte[] endKey;
    @Getter
    private final boolean isNotBetween;
    @Getter
    private final boolean includeStart;
    @Getter
    private final boolean includeEnd;

    public DingoPartRangeScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        @Nullable RexNode filter,
        @Nullable TupleMapping selection,
        byte[] startKey,
        byte[] endKey,
        boolean isNotBetween,
        boolean includeStart,
        boolean includeEnd
    ) {
        this(
            cluster,
            traitSet,
            hints,
            table,
            filter,
            selection,
            null,
            null,
            null,
            startKey,
            endKey,
            isNotBetween,
            includeStart,
            includeEnd,
            false
        );
    }

    public DingoPartRangeScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        @Nullable RexNode filter,
        @Nullable TupleMapping selection,
        @Nullable List<AggregateCall> aggCalls,
        @Nullable ImmutableBitSet groupSet,
        @Nullable ImmutableList<ImmutableBitSet> groupSets,
        byte[] startKey,
        byte[] endKey,
        boolean isNotBetween,
        boolean includeStart,
        boolean includeEnd,
        boolean pushDown
    ) {
        super(cluster, traitSet, hints, table, filter, selection, aggCalls, groupSet, groupSets, pushDown);
        this.startKey = startKey;
        this.endKey = endKey;
        this.isNotBetween = isNotBetween;
        this.includeStart = includeStart;
        this.includeEnd = includeEnd;
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(@NonNull RelOptPlanner planner, @NonNull RelMetadataQuery mq) {
        // Assume that part scan has half cost.
        return Objects.requireNonNull(super.computeSelfCost(planner, mq)).multiplyBy(0.5d);
    }

    @Override
    public @NonNull RelWriter explainTerms(@NonNull RelWriter pw) {
        super.explainTerms(pw);
        // crucial, this is how Calcite distinguish between different node with different props.
        pw.item("startKey", startKey);
        pw.item("endKey", endKey);
        pw.item("isNotBetween", isNotBetween);
        pw.item("includeStart", includeStart);
        pw.item("includeEnd", includeEnd);
        return pw;
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public static @NonNull DingoPartRangeScan of(@NonNull DingoTableScan tableScan) {
        return new DingoPartRangeScan(
            tableScan.getCluster(),
            tableScan.getTraitSet(),
            tableScan.getHints(),
            tableScan.getTable(),
            tableScan.getFilter(),
            tableScan.getSelection(),
            tableScan.getAggCalls(),
            tableScan.getGroupSet(),
            tableScan.getGroupSets(),
            null,
            null,
            false,
            true,
            false,
            tableScan.isPushDown()
        );
    }
}
