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

package io.dingodb.calcite.stats;

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import org.apache.calcite.plan.RelOptTable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class StatsCache {
    public static volatile Map<String, TableStats> statsMap = new ConcurrentHashMap<>();

    private StatsCache() {
    }

    public static double getTableRowCount(String schemaName, String tableName) {
        return getTableRowCount(schemaName + "." + tableName);
    }

    public static double getTableRowCount(LogicalDingoTableScan scan) {
        return getTableRowCount(scan.getTable());
    }

    public static double getTableRowCount(String key) {
        TableStats tableStats = statsMap.get(key);
        if (tableStats != null) {
            return tableStats.getRowCount();
        }
        return 100;
    }

    public static double getTableRowCount(RelOptTable relOptTable) {
        DingoTable dingoTable = relOptTable.unwrap(DingoTable.class);
        assert dingoTable != null;
        if (dingoTable.getNames().size() > 2) {
            return getTableRowCount(dingoTable.getNames().get(1), dingoTable.getNames().get(2));
        } else {
            return 100;
        }
    }

    public static TableStats getStatistic(String schemaName, String tableName) {
        return statsMap.get(schemaName + "." + tableName);
    }
}
