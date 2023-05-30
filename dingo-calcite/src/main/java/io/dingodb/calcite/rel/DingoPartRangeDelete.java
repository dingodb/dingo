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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public final class DingoPartRangeDelete extends AbstractRelNode implements DingoRel {
    @Getter
    private final RelOptTable table;
    private final RelDataType rowType;
    @Getter
    private final byte[] startKey;
    @Getter
    private final byte[] endKey;
    @Getter
    private final boolean notBetween;
    @Getter
    private final boolean includeStart;
    @Getter
    private final boolean includeEnd;

    public DingoPartRangeDelete(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable table,
        RelDataType rowType,
        byte[] startKey,
        byte[] endKey,
        boolean notBetween,
        boolean includeStart,
        boolean includeEnd
    ) {
        super(cluster, traits);
        this.table = table;
        this.rowType = rowType;
        this.startKey = startKey;
        this.endKey = endKey;
        this.notBetween = notBetween;
        this.includeStart = includeStart;
        this.includeEnd = includeEnd;
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return Objects.requireNonNull(super.computeSelfCost(planner, mq)).multiplyBy(0.5d);
    }

    @Override
    public @NonNull RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("table", table.getQualifiedName());
        pw.item("startKey", startKey);
        pw.item("endKey", endKey);
        pw.item("includeStart", includeStart);
        pw.item("includeEnd", includeEnd);
        return pw;
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
