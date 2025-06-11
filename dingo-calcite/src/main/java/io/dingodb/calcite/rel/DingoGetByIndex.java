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

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.stats.StatsCache;
import io.dingodb.calcite.utils.DingoFilterUtils;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.Pair;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.dingodb.calcite.meta.DingoCostModelV1.getAvgRowSize;
import static io.dingodb.calcite.meta.DingoCostModelV1.getScanAvgRowSize;
import static io.dingodb.calcite.meta.DingoCostModelV1.getScanCost;
import static io.dingodb.calcite.meta.DingoCostModelV1.getSelectionCdList;
import static io.dingodb.calcite.meta.DingoCostModelV1.needLookUp;
import static io.dingodb.calcite.meta.DingoCostModelV1.netFactor;
import static io.dingodb.calcite.meta.DingoCostModelV1.scanConcurrency;
import static io.dingodb.calcite.meta.DingoCostModelV1.scanFactor;
import static io.dingodb.common.mysql.scope.ScopeVariables.getLookupConcurrency;

@Slf4j
public class DingoGetByIndex extends LogicalDingoTableScan implements DingoRel {
    @Getter
    private double rowCount;

    @Getter
    protected final Map<CommonId, Set> indexSetMap;
    @Getter
    protected final Map<CommonId, Table> indexTdMap;

    @Getter
    private final boolean isUnique;
    private final boolean lookup;

    public DingoGetByIndex(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        RexNode filter,
        @Nullable TupleMapping selection,
        boolean isUnique,
        Map<CommonId, Set> indexSetMap,
        Map<CommonId, Table> indexTdMap
    ) {
        super(cluster, traitSet, hints, table, filter, selection);
        this.indexSetMap = indexSetMap;
        this.indexTdMap = indexTdMap;
        this.isUnique = isUnique;
        this.lookup = isLookup();
    }

    public DingoGetByIndex(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        RexNode filter,
        @Nullable TupleMapping selection,
        boolean isUnique,
        Map<CommonId, Set> indexSetMap,
        Map<CommonId, Table> indexTdMap,
        boolean forDml
    ) {
        super(cluster, traitSet, hints, table, filter, selection, null, null, null, false, forDml);
        this.indexSetMap = indexSetMap;
        this.indexTdMap = indexTdMap;
        this.isUnique = isUnique;
        this.lookup = isLookup();
    }

    @Override
    public @NonNull RelWriter explainTerms(@NonNull RelWriter pw) {
        super.explainTerms(pw);
        pw.item("points", indexSetMap);
        pw.itemIf("lookup", lookup, true);
        return pw;
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(@NonNull RelOptPlanner planner, @NonNull RelMetadataQuery mq) {
        DingoTable dingoTable = getTable().unwrap(DingoTable.class);
        assert dingoTable != null;
        String schemaName = dingoTable.getNames().get(1);

        CommonId commonId = getIndexSetMap().keySet().stream().findFirst().get();
        Table indexTd = getIndexTdMap().get(commonId);

        List<Column> indexCdList = indexTd.getColumns();
        double indexRowSize = getAvgRowSize(indexCdList,
            dingoTable.getTable(), schemaName);

        double estimateRowCount = estimateRowCount(mq);
        double indexScanCost = estimateRowCount * (Math.log(indexRowSize) / Math.log(2)) * scanFactor;
        double indexNetCost = estimateRowCount * indexRowSize * netFactor;
        double cost = (indexNetCost + indexScanCost) / scanConcurrency;
        if (isLookup()) {
            double rowSize = getScanAvgRowSize(this);
            double tableScanCost = getScanCost(estimateRowCount, rowSize);
            double tableNetCost = estimateRowCount * rowSize * netFactor;
            tableScanCost += tableNetCost;
            tableScanCost += estimateRowCount * ScopeVariables.getSeekFactor();
            cost += (tableScanCost) / getLookupConcurrency();
        }
        return DingoCost.FACTORY.makeCost(cost * 0.7, 0, 0);
    }

    public boolean isLookup() {
        if (getIndexSetMap() == null) {
            return true;
        }
        CommonId commonId = getIndexSetMap().keySet().stream().findFirst().get();
        Table indexTd = getIndexTdMap().get(commonId);

        List<Column> indexCdList = indexTd.getColumns();
        DingoTable dingoTable = getTable().unwrap(DingoTable.class);
        List<Column> selectionCdList = getSelectionCdList(this, dingoTable);
        return needLookUp(indexCdList, selectionCdList);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        CommonId commonId = getIndexSetMap().keySet().stream().findFirst().orElse(null);
        if (commonId == null) {
            return ScopeVariables.getStatsDefaultCount();
        }
        Table indexTd = getIndexTdMap().get(commonId);
        Table td = Objects.requireNonNull(getTable().unwrap(DingoTable.class)).getTable();
        List<Integer> indexSelectionList = td.getColumnIndices2(indexTd.getColumns());
        Mapping mapping = Mappings.target(indexSelectionList, td.getColumns().size());
        Pair<RexNode, RexNode> res = DingoFilterUtils.splitRexFilter(filter, mapping, getCluster().getRexBuilder());
        RexNode indexFilter;
        if (res != null && res.getKey() != null) {
            indexFilter = res.getKey();
        } else {
            indexFilter = filter;
        }

        double rowCount;
        if (indexFilter != null) {
            RelNode fakeInput = new LogicalDingoTableScan(
                getCluster(),
                getTraitSet(),
                getHints(),
                table,
                null,
                null
            );
            rowCount = RelMdUtil.estimateFilteredRows(fakeInput, indexFilter, mq);
            if (rowCount < 1) {
                rowCount = 1;
            }
        } else {
            rowCount = StatsCache.getTableRowCount(this);
        }
        if (groupSet != null) {
            if (groupSet.cardinality() == 0) {
                rowCount = 1.0;
            } else {
                rowCount *= 1.0 - Math.pow(.8, groupSet.cardinality());
            }
        }
        this.rowCount = rowCount;
        return rowCount;
    }
}
