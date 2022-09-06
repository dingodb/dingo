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
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import javax.annotation.Nonnull;

public class DingoPartRangeScan extends AbstractRelNode implements DingoRel {
    @Getter
    private final RelOptTable table;
    @Getter
    private final RexNode filter;
    @Getter
    private final TupleMapping selection;
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
        @javax.annotation.Nullable RexNode filter,
        @javax.annotation.Nullable TupleMapping selection,
        byte[] startKey,
        byte[] endKey,
        boolean isNotBetween,
        boolean includeStart,
        boolean includeEnd
    ) {
        super(cluster, traitSet);
        this.table = table;
        this.filter = filter;
        this.selection = selection;
        this.startKey = startKey;
        this.endKey = endKey;
        this.isNotBetween = isNotBetween;
        this.includeStart = includeStart;
        this.includeEnd = includeEnd;
    }

    @Override
    protected RelDataType deriveRowType() {
        return mapRowType(table.getRowType(), selection);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(@Nonnull RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Nonnull
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        // crucial, this is how Calcite distinguish between different node with different props.
        pw.item("table", table.getQualifiedName());
        pw.itemIf("filter", filter, filter != null && ((RexCall) filter).op.kind == SqlKind.AND);
        pw.itemIf("selection", selection, selection != null);
        return pw;
    }

    @Override
    public <T> T accept(@Nonnull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
