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
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.stats.StatsCache;
import io.dingodb.calcite.stats.TableStats;
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
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DingoCostModelV1 extends DingoCostModel {

    public static final double scanFactor = 40.7;
    public static final double netFactor = 3.96;
    public static final double cpuFactor = 49.9;
    public static final double scanConcurrency = 1;
    public static final double lookupConcurrency = 1;

    public static final double memFactor = 0.01;

    private static DingoCostModelV1 INSTANCE;

    public static synchronized DingoCostModelV1 getCostModel() {
        if (INSTANCE == null) {
            INSTANCE = new DingoCostModelV1();
        }

        return INSTANCE;
    }

    public static double getScanAvgRowSize(LogicalDingoTableScan tableScan) {
        DingoTable dingoTable = tableScan.getTable().unwrap(DingoTable.class);
        assert dingoTable != null;
        String schemaName = dingoTable.getNames().get(1);
        //List<Column> selectionCdList = getSelectionCdList(tableScan, dingoTable);
        return getAvgRowSize(
            dingoTable.getTable().columns, dingoTable.getTable(), schemaName
        );
    }

    public static double getScanCost(double rowCount, double rowSize) {
        double cost = rowCount * (Math.log(rowSize) / Math.log(2)) * scanFactor;
        if (Double.isInfinite(cost)) {
            return 0;
        } else {
            return cost;
        }
    }

    public static double getNetCost(double rowCount, double rowSize) {
        return rowCount * rowSize * netFactor;
    }

    @NonNull
    public static List<Column> getSelectionCdList(LogicalDingoTableScan tableScan, DingoTable dingoTable) {
        if (tableScan.getRealSelection() == null) {
            return dingoTable.getTable().getColumns();
        }
        if (tableScan.getSelection() == null) {
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

    public static boolean needLookUp(List<Column> indexCdList, List<Column> selectionCdList) {
        if (selectionCdList.size() > indexCdList.size()) {
            return true;
        } else {
            return !selectionCdList.stream().allMatch(indexCdList::contains);
        }
    }
}
