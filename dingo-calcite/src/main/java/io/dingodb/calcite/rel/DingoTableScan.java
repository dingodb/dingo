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
import io.dingodb.calcite.utils.RelDataTypeUtils;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import io.dingodb.common.type.TupleMapping;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class DingoTableScan extends LogicalDingoTableScan implements DingoRel {

    public DingoTableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        @Nullable RexNode filter,
        @Nullable TupleMapping selection
    ) {
        this(cluster, traitSet, hints, table, filter, selection, null, null, null, false, false);
    }

    public DingoTableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        @Nullable RexNode filter,
        @Nullable TupleMapping selection,
        @Nullable List<AggregateCall> aggCalls,
        @Nullable ImmutableBitSet groupSet,
        @Nullable ImmutableList<ImmutableBitSet> groupSets,
        boolean pushDown,
        boolean forDml
    ) {
        super(cluster, traitSet, hints, table, filter, selection, aggCalls, groupSet, groupSets, pushDown, forDml);
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DingoTableScan(
            getCluster(),
            traitSet,
            hints,
            table,
            filter,
            selection,
            aggCalls,
            groupSet,
            groupSets,
            pushDown,
            forDml
        );
    }

    public RelDataType getNormalRowType() {
        RelDataType selected = getNormalSelectedType();
        if (aggCalls != null) {
            return Aggregate.deriveRowType(
                getCluster().getTypeFactory(),
                selected,
                false,
                groupSet,
                groupSets,
                aggCalls
            );
        }
        return selected;
    }

    private RelDataType getNormalSelectedType() {
        return RelDataTypeUtils.mapType(
            getCluster().getTypeFactory(),
            getTableType(),
            selection
        );
    }
}
