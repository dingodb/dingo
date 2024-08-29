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

import io.dingodb.calcite.rel.DingoRel;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;

public final class DingoSort extends Sort implements DingoRel {
    @Getter
    private double rowCount;

    public DingoSort(
        RelOptCluster cluster,
        RelTraitSet traits,
        List<RelHint> hints,
        RelNode child,
        RelCollation collation,
        @Nullable RexNode offset,
        @Nullable RexNode fetch
    ) {
        super(cluster, traits, hints, child, collation, offset, fetch);
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Sort copy(
        RelTraitSet traitSet,
        RelNode newInput,
        RelCollation newCollation,
        @Nullable RexNode offset,
        @Nullable RexNode fetch
    ) {
        return new DingoSort(getCluster(), traitSet, this.hints, newInput, newCollation, offset, fetch);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double offsetValue = Util.first(doubleValue(this.offset), 0.0);

        assert offsetValue >= 0.0 : "offset should not be negative:" + offsetValue;

        double inCount = estimateRowCount(mq);
        Double fetchValue = doubleValue(this.fetch);
        double readCount;
        if (fetchValue == null) {
            readCount = inCount;
        } else {
            if (fetchValue <= 0.0) {
                return planner.getCostFactory().makeCost(inCount, 0.0, 0.0);
            }

            readCount = Math.min(inCount, offsetValue + fetchValue);
        }

        double bytesPerRow = (3 + this.getRowType().getFieldCount()) * 4;
        double cpu;
        if (this.collation.getFieldCollations().isEmpty()) {
            cpu = readCount * bytesPerRow;
        } else {
            cpu = Util.nLogM(inCount, readCount) * bytesPerRow;
        }

        return planner.getCostFactory().makeCost(readCount, cpu, 0.0);
    }

    private static @Nullable Double doubleValue(@Nullable RexNode r) {
        return r instanceof RexLiteral ? ((RexLiteral)r).getValueAs(Double.class) : null;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        rowCount = super.estimateRowCount(mq);
        return rowCount;
    }
}
