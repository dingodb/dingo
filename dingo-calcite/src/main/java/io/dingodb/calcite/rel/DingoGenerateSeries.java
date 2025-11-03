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

import io.dingodb.calcite.DingoRelOptTable;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import io.dingodb.common.type.TupleMapping;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class DingoGenerateSeries extends LogicalGenerateSeries implements DingoRel {

    @Getter
    private double rowCount;

    public DingoGenerateSeries(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RexCall call,
        DingoRelOptTable table,
        List<Object> operands,
        TupleMapping selection,
        RexNode filter
    ) {
        super(cluster, traitSet, call, table, operands, selection, filter);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return DingoCost.FACTORY.makeCost(Integer.MAX_VALUE, 0, 0);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);

        return pw;
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        rowCount = super.estimateRowCount(mq);
        return rowCount;
    }
}
