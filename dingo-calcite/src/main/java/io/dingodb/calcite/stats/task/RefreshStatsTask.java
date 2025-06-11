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

package io.dingodb.calcite.stats.task;

import io.dingodb.calcite.stats.CountMinSketch;
import io.dingodb.calcite.stats.Histogram;
import io.dingodb.calcite.stats.StatsCache;
import io.dingodb.calcite.stats.StatsNormal;
import io.dingodb.calcite.stats.StatsOperator;
import io.dingodb.calcite.stats.TableStats;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.session.Session;
import io.dingodb.common.session.SessionUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * load dingo stats. contain histogram and cmsketch
 */
@Slf4j
public class RefreshStatsTask extends StatsOperator implements Runnable {

    @Override
    public void run() {
        LogUtils.info(log, "refresh stats start");
        Map<String, TableStats> statsMap = new ConcurrentHashMap<>();
        List<RangeDistribution> rangeDistributions = new ArrayList<>(metaService
            .getRangeDistribution(bucketsTblId).values());
        List<Object[]> values;
        try {
            values = scan(bucketsStore, bucketsCodec, rangeDistributions.get(0));
        } catch (Exception e) {
            values = new ArrayList<>();
        }
        Set<Long> invalidList = new HashSet<>();
        values.forEach(e -> {
            String histogramStr = (String) e[4];
            Histogram histogram = Histogram.deserialize(histogramStr);
            if (histogram != null) {
                histogram.setTableId((Long) e[3]);
            }
            String histogramKey = e[0] + "." + e[1];
            TableStats tableStats = statsMap.computeIfPresent(histogramKey,
                (k, v) -> {
                    v.getHistogramList().add(histogram);
                    return v;
                });
            if (tableStats == null) {
                tableStats = new TableStats((Long)e[3], (String) e[0], (String) e[1]);
                if (StatsCache.validate(tableStats.getTableId())) {
                    tableStats.getHistogramList().add(histogram);
                    statsMap.putIfAbsent(histogramKey, tableStats);
                } else {
                    invalidList.add(tableStats.getTableId());
                }
            }
        });

        rangeDistributions = new ArrayList<>(metaService
            .getRangeDistribution(cmSketchTblId).values());
        try {
            values = scan(cmSketchStore, cmSketchCodec, rangeDistributions.get(0));
        } catch (Exception e) {
            values = new ArrayList<>();
        }
        values.forEach(e -> {
            String cmSKetchStr = (String) e[4];
            CountMinSketch countMinSketch = CountMinSketch.deserialize(cmSKetchStr);
            countMinSketch.setColumnName((String) e[2]);
            countMinSketch.setSchemaName((String) e[0]);
            countMinSketch.setTableName((String) e[1]);
            countMinSketch.setNullCount((Long) e[5]);
            countMinSketch.setTotalCount((Long) e[6]);
            countMinSketch.setIndex((Integer) e[7]);
            countMinSketch.setTableId((Long)e[3]);
            if (!StatsCache.validate(countMinSketch.getTableId())) {
                invalidList.add(countMinSketch.getTableId());
                return;
            }
            String cmSketchKey = e[0] + "." + e[1];
            TableStats tableStats = statsMap.computeIfPresent(cmSketchKey,
                (k, v) -> {
                    v.getCountMinSketchList().add(countMinSketch);
                    return v;
                });
            if (tableStats == null) {
                tableStats = new TableStats((Long)e[3], (String) e[0], (String) e[1]);
                tableStats.getCountMinSketchList().add(countMinSketch);
                statsMap.putIfAbsent(cmSketchKey, tableStats);
            }
        });

        rangeDistributions = new ArrayList<>(metaService.getRangeDistribution(statsTblId).values());
        try {
            values = scan(statsStore, statsCodec, rangeDistributions.get(0));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            values = new ArrayList<>();
        }
        values.forEach(e -> {
            String statsNormalKey = e[0] + "." + e[1];
            StatsNormal statsNormal = new StatsNormal((String) e[2], (Long) e[4], (Long) e[5],
                (Long) e[6], (Long) e[7]);
            long tableId = (Long)e[3];
            if (!StatsCache.validate(tableId)) {
                invalidList.add(tableId);
                return;
            }
            TableStats tableStats = statsMap.computeIfPresent(statsNormalKey,
                (k, v) -> {
                    v.getStatsNormalList().add(statsNormal);
                    return v;
                });
            if (tableStats == null) {
                tableStats = new TableStats(tableId, (String) e[0], (String) e[1]);
                if (!StatsCache.validate(tableStats.getTableId())) {
                    invalidList.add(tableStats.getTableId());
                    return;
                }
                tableStats.getStatsNormalList().add(statsNormal);
                statsMap.putIfAbsent(statsNormalKey, tableStats);
            }
        });
        statsMap.values().forEach(TableStats::initRowCount);
        StatsCache.statsMap = statsMap;
        if (log.isDebugEnabled()) {
            log.debug("load stats" + statsMap);
        }
        cleanInvalidStats(invalidList);
        LogUtils.info(log, "refresh stats end");
    }

    public static void cleanInvalidStats(Set<Long> invalidList) {
        if (invalidList == null) {
            return;
        }
        if (invalidList.isEmpty()) {
            return;
        }
        String delBucketSql = "delete from mysql.table_buckets where table_id=";
        String delNormalSql = "delete from mysql.table_stats where table_id=";
        String delCmSketchSql = "delete from mysql.cm_sketch where table_id=";
        delStats(delBucketSql, invalidList);
        delStats(delNormalSql, invalidList);
        delStats(delCmSketchSql, invalidList);
    }

    public static void delStats(String sql, Set<Long> tableIdList) {
        Session session = SessionUtil.INSTANCE.getSession();
        try {
            tableIdList.forEach(tableId -> {
                String sqlTmp = sql + tableId;
                session.executeUpdate(sqlTmp);
            });
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        } finally {
            session.destroy();
        }
    }
}
