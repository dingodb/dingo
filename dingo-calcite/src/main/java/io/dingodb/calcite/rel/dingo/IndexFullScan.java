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
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.rel.logical.LogicalIndexFullScan;
import io.dingodb.calcite.stats.StatsCache;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class IndexFullScan extends LogicalIndexFullScan implements DingoRel {

    @Getter
    private double rowCount;
    @Getter
    private double fullRowCount;

    public IndexFullScan(RelOptCluster cluster,
                         RelTraitSet traitSet,
                         List<RelHint> hints,
                         RelOptTable table,
                         @Nullable RexNode filter,
                         @Nullable TupleMapping selection,
                         Table indexTable,
                         CommonId indexId,
                         List<Integer> selectionIndexList,
                         boolean pushDown,
                         boolean lookup,
                         int keepSerialOrder
                        ) {
        super(
            cluster, traitSet, hints, table, filter,
           selection, indexTable, indexId, selectionIndexList, pushDown, lookup, keepSerialOrder
           );
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(@NonNull RelOptPlanner planner, @NonNull RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double rowCount = 0;
        if (filter != null) {
            RelNode fakeInput = new LogicalDingoTableScan(
                getCluster(),
                getTraitSet(),
                getHints(),
                table,
                null,
                null
            );
            rowCount = RelMdUtil.estimateFilteredRows(fakeInput, filter, mq);
            if (rowCount < 1) {
                rowCount = 1;
            }
        } else {
            rowCount = StatsCache.getTableRowCount(this.table);
        }
        this.fullRowCount = StatsCache.getTableRowCount(this.table);
        this.rowCount = rowCount;
        return rowCount;
    }
}
