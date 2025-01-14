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

package io.dingodb.calcite.rel.logical;

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static io.dingodb.calcite.meta.DingoCostModelV1.getAvgRowSize;
import static io.dingodb.calcite.meta.DingoCostModelV1.getScanAvgRowSize;
import static io.dingodb.calcite.meta.DingoCostModelV1.getScanCost;
import static io.dingodb.calcite.meta.DingoCostModelV1.netFactor;
import static io.dingodb.calcite.meta.DingoCostModelV1.scanConcurrency;
import static io.dingodb.calcite.meta.DingoCostModelV1.scanFactor;

public class LogicalDocumentScanFilter extends LogicalDingoTableScan {
    @Getter
    private CommonId indexId;
    @Getter
    private Table indexTable;
    @Getter
    private boolean lookup;
    @Getter
    @Setter
    private int keepSerialOrder;
    @Getter
    private String queryString;

    public LogicalDocumentScanFilter(RelOptCluster cluster,
                                     RelTraitSet traitSet,
                                     List<RelHint> hints,
                                     RelOptTable table,
                                     @Nullable RexNode filter,
                                     @Nullable TupleMapping selection,
                                     Table indexTable,
                                     CommonId indexId,
                                     boolean pushDown,
                                     boolean lookup,
                                     int keepSerialOrder,
                                     String queryString) {
        super(cluster, traitSet, hints, table, filter, selection, null, null, null, pushDown, false);
        this.indexTable = indexTable;
        this.indexId = indexId;
        this.lookup = lookup;
        this.keepSerialOrder = keepSerialOrder;
        this.queryString = queryString;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return super.estimateRowCount(mq);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(@NonNull RelOptPlanner planner, @NonNull RelMetadataQuery mq) {
        DingoTable dingoTable = table.unwrap(DingoTable.class);

        assert dingoTable != null;
        String schemaName = "";
        if (dingoTable.getNames().size() > 2) {
            schemaName = dingoTable.getNames().get(1);
        }
        double indexRowSize = getAvgRowSize(indexTable.getColumns(),
            dingoTable.getTable(), schemaName);

        double rowCount = estimateRowCount(mq);

        double indexScanCost = rowCount * (Math.log(indexRowSize) / Math.log(2)) * scanFactor;
        double indexNetCost = rowCount * indexRowSize * netFactor;
        double cost = (indexNetCost + indexScanCost) / scanConcurrency;

        if (lookup) {
            double rowSize = getScanAvgRowSize(this);
            double estimateRowCount = estimateRowCount(mq);
            double tableScanCost = getScanCost(estimateRowCount, rowSize);
            double tableNetCost = estimateRowCount * rowSize * netFactor;
            cost += (tableScanCost + tableNetCost) / scanConcurrency;
        }

        return planner.getCostFactory().makeCost(cost * 0.8, 0, 0);
    }

    @Override
    public @NonNull RelWriter explainTerms(@NonNull RelWriter pw) {
        // crucial, this is how Calcite distinguish between different node with different props.
        pw.itemIf("filter", filter, filter != null);
        pw.itemIf("selection", selection, selection != null);
        pw.itemIf("groupSet", groupSet, groupSet != null);
        pw.itemIf("aggCalls", aggCalls, aggCalls != null);
        pw.itemIf("pushDown", pushDown, pushDown);
        pw.itemIf("lookup", lookup, true);
        pw.itemIf("queryString", queryString, !queryString.equals(""));
        return pw;
    }
}
