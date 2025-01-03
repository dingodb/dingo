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

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoCost;
import io.dingodb.calcite.rel.DingoRel;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.rel.logical.LogicalIndexScanWithRelOp;
import io.dingodb.calcite.stats.StatsCache;
import io.dingodb.calcite.utils.RangeUtils;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.meta.entity.IndexTable;
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

import static io.dingodb.calcite.meta.DingoCostModelV1.getAvgRowSize;
import static io.dingodb.calcite.meta.DingoCostModelV1.getNetCost;
import static io.dingodb.calcite.meta.DingoCostModelV1.getScanCost;
import static io.dingodb.calcite.meta.DingoCostModelV1.scanConcurrency;

public class DingoIndexScanWithRelOp extends LogicalIndexScanWithRelOp implements DingoRel {
    @Getter
    private double rowCount;
    @Getter
    private double fullRowCount;
    @Getter
    RangeDistribution rangeDistribution;

    public DingoIndexScanWithRelOp(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        RelDataType rowType,
        RelOp relOp,
        RexNode filter,
        boolean pushDown,
        int keepOrder,
        IndexTable indexTable,
        boolean rangeScan
    ) {
        super(cluster, traitSet, hints, table, rowType, relOp, filter, pushDown, keepOrder, indexTable, rangeScan);
        if (getFilter() != null) {
            KeyValueCodec codec = CodecService.getDefault()
                .createKeyValueCodec(indexTable.getCodecVersion(), indexTable.version, indexTable.tupleType(),
                indexTable.keyMapping());
            RangeDistribution range = RangeUtils.createRangeByFilter(indexTable, codec, filter, null);
            rangeDistribution = range;
            if (range != null && !(range.getStartKey() == null && range.getEndKey() == null)) {
                this.rangeScan = true;
            }
        }
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visitDingoIndexScanWithRelOp(this);
    }

    @Override
    public @NonNull RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DingoIndexScanWithRelOp(
            getCluster(),
            traitSet,
            hints,
            table,
            rowType,
            relOp,
            filter,
            pushDown,
            keepSerialOrder,
            indexTable,
            rangeScan
        );
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

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount;
        if (!rangeScan) {
            rowCount = StatsCache.getTableRowCount(table);
        } else {
            rowCount = this.estimateRowCount(mq);
        }
        DingoTable dingoTable = table.unwrap(DingoTable.class);
        assert dingoTable != null;
        String schemaName = dingoTable.getNames().get(1);
        double rowSize = getAvgRowSize(indexTable.columns, indexTable, schemaName);
        double tableScanCost = getScanCost(rowCount, rowSize);
        double tableNetCost = getNetCost(rowCount, rowSize);
        double rangeCost = (tableScanCost + tableNetCost) / scanConcurrency;
        return DingoCost.FACTORY.makeCost(rangeCost * 0.7, 0, 0);
    }

}
