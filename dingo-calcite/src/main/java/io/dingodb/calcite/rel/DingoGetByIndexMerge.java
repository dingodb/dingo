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
import io.dingodb.common.CommonId;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.entity.Table;
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
import java.util.Map;
import java.util.Set;

import static io.dingodb.calcite.meta.DingoCostModelV1.memFactor;

public class DingoGetByIndexMerge extends DingoGetByIndex implements DingoRel {
    @Getter
    private double rowCount;

    @Getter
    private TupleMapping keyMapping;

    public DingoGetByIndexMerge(RelOptCluster cluster,
                                RelTraitSet traitSet,
                                List<RelHint> hints,
                                RelOptTable table,
                                RexNode filter,
                                @Nullable TupleMapping selection,
                                boolean isUnique,
                                Map<CommonId, Set> indexSetMap,
                                Map<CommonId, Table> indexTdMap,
                                TupleMapping keyMapping) {
        super(cluster, traitSet, hints, table, filter, selection, isUnique, indexSetMap, indexTdMap);
        this.keyMapping = keyMapping;
    }

    public DingoGetByIndexMerge(RelOptCluster cluster,
                                RelTraitSet traitSet,
                                List<RelHint> hints,
                                RelOptTable table,
                                RexNode filter,
                                @Nullable TupleMapping selection,
                                boolean isUnique,
                                Map<CommonId, Set> indexSetMap,
                                Map<CommonId, Table> indexTdMap,
                                TupleMapping keyMapping,
                                boolean forDml) {
        super(cluster, traitSet, hints, table, filter, selection, isUnique, indexSetMap, indexTdMap, forDml);
        this.keyMapping = keyMapping;
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(@NonNull RelOptPlanner planner, @NonNull RelMetadataQuery mq) {
        RelOptCost cost = super.computeSelfCost(planner, mq);
        double rowCount = this.estimateRowCount(mq);
        RelOptCost memCost = DingoCost.FACTORY.makeCost(rowCount * memFactor, 0, 0);
        assert cost != null;
        return cost.plus(memCost);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        rowCount = super.estimateRowCount(mq);
        return rowCount;
    }
}
