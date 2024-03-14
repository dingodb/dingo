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

package io.dingodb.calcite.meta;

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoCost;
import io.dingodb.calcite.rel.DingoGetByIndex;
import io.dingodb.calcite.rel.DingoGetByIndexMerge;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.stats.StatsCache;
import io.dingodb.calcite.stats.TableStats;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.scalar.DateType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.common.type.scalar.IntegerType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimeType;
import io.dingodb.common.type.scalar.TimestampType;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class DingoCostModelV1 extends DingoCostModel {

    private static final double scanFactor = 40.7;
    private static final double netFactor = 3.96;
    private static final double requestFactor = 60;
    private static final double cpuFactor = 49.9;
    public static final double scanConcurrency = 15;
    private static final double lookupConcurrency = 5;

    private static final double memFactor = 0.01;

    private static DingoCostModelV1 INSTANCE;

    public static synchronized DingoCostModelV1 getCostModel() {
        if (INSTANCE == null) {
            INSTANCE = new DingoCostModelV1();
        }

        return INSTANCE;
    }

    @Override
    public RelOptCost getDingoGetByIndex(DingoGetByIndex dingoGetByIndex, RelMetadataQuery mq) {
        // cost = index_cost + (table_cost + double_read_cost) / double_read_concurrency
        // index cost = (index_scan_cost + index_net_cost) / dist_concurrency
        // table cost = (table_scan_cost + table_net_cost) / dist_concurrency

        // index_scan_cost = cardinality * log2(row-size) * scanFactor
        // table_scan_cost = cardinality * log2(row_size) * scanFactor

        double rowSize = getScanAvgRowSize(dingoGetByIndex);
        DingoTable dingoTable = dingoGetByIndex.getTable().unwrap(DingoTable.class);
        assert dingoTable != null;
        String schemaName = dingoTable.getNames().get(1);

        CommonId commonId = dingoGetByIndex.getIndexSetMap().keySet().stream().findFirst().get();
        Table table = dingoGetByIndex.getIndexTdMap().get(commonId);

        List<String> columnList = table.getColumns()
            .stream().map(Column::getName).collect(Collectors.toList());
        List<Column> indexCdList = dingoTable.getTable().getColumns().stream()
            .filter(cd -> columnList.contains(cd.getName())).collect(Collectors.toList());
        double indexRowSize = getAvgRowSize(indexCdList,
            dingoTable.getTable(), schemaName);

        double estimateRowCount = dingoGetByIndex.estimateRowCount(mq);
        double indexScanCost = estimateRowCount * (Math.log(indexRowSize) / Math.log(2)) * scanFactor;
        double indexNetCost = estimateRowCount * indexRowSize * netFactor;
        double indexSideCost = (indexNetCost + indexScanCost) / scanConcurrency;
        List<Column> selectionCdList = getSelectionCdList(dingoGetByIndex, dingoTable);
        boolean isNeedLookup = needLookUp(indexCdList, selectionCdList);
        double tableSideCost = 0;
        double doubleReadCost = 0;
        if (isNeedLookup) {
            double tableScanCost = getScanCost(estimateRowCount, rowSize);
            double tableNetCost = estimateRowCount * rowSize * netFactor;
            tableSideCost = (tableScanCost + tableNetCost) / scanConcurrency;

            double doubleReadTasks = estimateRowCount / 20000 * 32;
            double doubleReadRequestCost = doubleReadTasks * requestFactor;
            double doubleReadCpuCost = estimateRowCount * cpuFactor;
            doubleReadCost = doubleReadRequestCost + doubleReadCpuCost;
        }

        double cost = indexSideCost + (tableSideCost + doubleReadCost) / lookupConcurrency;
        return DingoCost.FACTORY.makeCost(cost, 0, 0);
    }

    public RelOptCost getDingoGetByIndexMerge(DingoGetByIndexMerge dingoGetByIndexMerge, RelMetadataQuery mq) {
        RelOptCost cost = getDingoGetByIndex(dingoGetByIndexMerge, mq);
        double rowCount = dingoGetByIndexMerge.estimateRowCount(mq);
        RelOptCost memCost = DingoCost.FACTORY.makeCost(rowCount * memFactor, 0, 0);
        return cost.plus(memCost);
    }

    public RelOptCost getDingoGetByKeys(DingoGetByKeys dingoGetByKeys, RelMetadataQuery mq) {
        double rowCount = dingoGetByKeys.estimateRowCount(mq);
        double rowSize = getScanAvgRowSize(dingoGetByKeys);
        double indexNetCost = getNetCost(rowCount, rowSize) / scanConcurrency;

        return DingoCost.FACTORY.makeCost(indexNetCost, 0, 0);
    }

    @Override
    public RelOptCost getDingoTableScan(DingoTableScan dingoTableScan, RelMetadataQuery mq) {
        return getLogicDingoTableScan(dingoTableScan, mq);
    }

    private RelOptCost getLogicDingoTableScan(LogicalDingoTableScan dingoTableScan, RelMetadataQuery mq) {
        double rowCount = dingoTableScan.getTable().getRowCount();
        if (rowCount == 0) {
            rowCount = StatsCache.getTableRowCount(dingoTableScan);
        }

        if (dingoTableScan.getGroupSet() != null) {
            if (dingoTableScan.getGroupSet().cardinality() == 0) {
                rowCount = 1.0;
            } else {
                rowCount *= 1.0 - Math.pow(.8, dingoTableScan.getGroupSet().cardinality());
            }
        }
        double rowSize = getScanAvgRowSize(dingoTableScan);
        double tableScanCost = getScanCost(rowCount, rowSize);
        double tableNetCost = getNetCost(rowCount, rowSize);
        double rangeCost = (tableScanCost + tableNetCost) / scanConcurrency;
        return DingoCost.FACTORY.makeCost(rangeCost, 0, 0);
    }

    private double getScanAvgRowSize(LogicalDingoTableScan tableScan) {
        DingoTable dingoTable = tableScan.getTable().unwrap(DingoTable.class);
        assert dingoTable != null;
        String schemaName = dingoTable.getNames().get(1);
        //List<Column> selectionCdList = getSelectionCdList(tableScan, dingoTable);
        return getAvgRowSize(
            dingoTable.getTable().columns, dingoTable.getTable(), schemaName
        );
    }

    public static double getScanCost(double rowCount, double rowSize) {
        Double cost = rowCount * (Math.log(rowSize) / Math.log(2)) * scanFactor;
        if (cost.isInfinite()) {
            return 0;
        } else {
            return cost;
        }
    }

    public static double getNetCost(double rowCount, double rowSize) {
        return rowCount * rowSize * netFactor;
    }

    @NonNull
    private static List<Column> getSelectionCdList(LogicalDingoTableScan tableScan, DingoTable dingoTable) {
        if (tableScan.getRealSelection() == null) {
            return dingoTable.getTable().getColumns();
        }
        int[] selections = tableScan.getSelection().getMappings();
        List<Column> selectionCdList = new ArrayList<>();
        for (int selection : selections) {
            selectionCdList.add(dingoTable.getTable().getColumns().get(selection));
        }
        return selectionCdList;
    }

    public static double getAvgRowSize(List<Column> selectionCds, Table td, String schemaName) {
        TableStats tableStats = StatsCache.getStatistic(schemaName, td.getName());
        AtomicLong avgRowSize = new AtomicLong();
        if (selectionCds == null) {
            selectionCds = td.getColumns();
        }
        selectionCds.forEach(cd -> {
            AtomicBoolean hasStats = new AtomicBoolean(false);
            if (tableStats != null) {
                tableStats.getStatsNormalList().forEach(statsNormal -> {
                    if (cd.getName().equals(statsNormal.getColumnName())) {
                        avgRowSize.addAndGet(statsNormal.getAvgColSize());
                        hasStats.set(true);
                    }
                });
            }
            if (!hasStats.get()) {
                avgRowSize.addAndGet(getTypeDefaultSize(cd));
            }
        });
        return avgRowSize.get();
    }

    public static long getTypeDefaultSize(Column columnDefinition) {
        if (columnDefinition.getType() instanceof IntegerType
            || columnDefinition.getType() instanceof FloatType
            || columnDefinition.getType() instanceof LongType
            || columnDefinition.getType() instanceof TimeType
            || columnDefinition.getType() instanceof DateType
            || columnDefinition.getType() instanceof TimestampType
        ) {
            return 4;
        } else if (columnDefinition.getType() instanceof DoubleType) {
            return 8;
        } else if (columnDefinition.getType() instanceof StringType) {
            int colLen = columnDefinition.getPrecision();
            if (colLen > 0) {
                if (colLen < 32) {
                    return colLen;
                } else if (colLen < 1000) {
                    return 32 + (colLen - 32) / 2;
                }
                return 32 + (1000 - 32) / 2;
            }
        }
        return 32;
    }

    private static boolean needLookUp(List<Column> indexCdList, List<Column> selectionCdList) {
        if (selectionCdList.size() > indexCdList.size()) {
            return true;
        } else {
            return !selectionCdList.stream().allMatch(indexCdList::contains);
        }
    }
}
