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
import io.dingodb.common.table.TupleMapping;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.annotation.Nonnull;

public final class DingoPartScan extends AbstractRelNode implements DingoRel {
    @Getter
    private final RelOptTable table;
    @Getter
    private final RexNode filter;
    @Getter
    private final TupleMapping selection;

    public DingoPartScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table
    ) {
        this(cluster, traitSet, table, null, null);
    }

    public DingoPartScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        @Nullable RexNode filter,
        @Nullable TupleMapping selection
    ) {
        super(cluster, traitSet);
        this.table = table;
        this.filter = filter;
        this.selection = selection;
    }

    @Override
    protected RelDataType deriveRowType() {
        return mapRowType(table.getRowType(), selection);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(@Nonnull RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = table.getRowCount();
        double cpu = rowCount + 1;
        double io = 0;
        return planner.getCostFactory().makeCost(rowCount, cpu, io);
    }

    @Nonnull
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        // crucial, this is how Calcite distinguish between different node with different props.
        pw.item("table", table);
        pw.itemIf("filter", filter, filter != null);
        pw.itemIf("selection", selection, selection != null);
        return pw;
    }

    @Override
    public <T> T accept(@Nonnull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
