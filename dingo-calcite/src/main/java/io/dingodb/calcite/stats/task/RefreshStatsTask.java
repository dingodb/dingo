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
import io.dingodb.common.partition.RangeDistribution;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * load dingo stats. contain histogram and cmsketch
 */
@Slf4j
public class RefreshStatsTask extends StatsOperator implements Runnable {

    @Override
    public void run() {
        Map<String, TableStats> statsMap = new ConcurrentHashMap<>();
        List<RangeDistribution> rangeDistributions = new ArrayList<>(metaService
            .getRangeDistribution(bucketsTblId).values());
        List<Object[]> values;
        try {
            values = scan(bucketsStore, bucketsCodec, rangeDistributions.get(0));
        } catch (Exception e) {
            values = new ArrayList<>();
        }
        values.forEach(e -> {
            String histogramStr = (String) e[3];
            Histogram histogram = Histogram.deserialize(histogramStr);
            String histogramKey = (e[0] + "." + e[1]).toUpperCase();
            TableStats tableStats = statsMap.computeIfPresent(histogramKey,
                (k, v) -> {
                    v.getHistogramList().add(histogram);
                    return v;
                });
            if (tableStats == null) {
                tableStats = new TableStats((String) e[0], (String) e[1]);
                tableStats.getHistogramList().add(histogram);
                statsMap.putIfAbsent(histogramKey, tableStats);
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
            String cmSKetchStr = (String) e[3];
            CountMinSketch countMinSketch = CountMinSketch.deserialize(cmSKetchStr);
            countMinSketch.setColumnName((String) e[2]);
            countMinSketch.setSchemaName((String) e[0]);
            countMinSketch.setTableName((String) e[1]);
            countMinSketch.setNullCount((Long) e[4]);
            countMinSketch.setTotalCount((Long) e[5]);
            countMinSketch.setIndex((Integer) e[6]);
            String cmSketchKey = (e[0] + "." + e[1]).toUpperCase();
            TableStats tableStats = statsMap.computeIfPresent(cmSketchKey,
                (k, v) -> {
                    v.getCountMinSketchList().add(countMinSketch);
                    return v;
                });
            if (tableStats == null) {
                tableStats = new TableStats((String) e[0], (String) e[1]);
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
            String statsNormalKey = (e[0] + "." + e[1]).toUpperCase();
            StatsNormal statsNormal = new StatsNormal((String) e[2], (Long) e[3], (Long) e[4],
                (Long) e[5], (Long) e[6]);
            TableStats tableStats = statsMap.computeIfPresent(statsNormalKey,
                (k, v) -> {
                    v.getStatsNormalList().add(statsNormal);
                    return v;
                });
            if (tableStats == null) {
                tableStats = new TableStats((String) e[0], (String) e[1]);
                tableStats.getStatsNormalList().add(statsNormal);
                statsMap.putIfAbsent(statsNormalKey, tableStats);
            }
        });
        statsMap.values().forEach(TableStats::initRowCount);
        StatsCache.statsMap = statsMap;
        if (log.isDebugEnabled()) {
            log.debug("load stats" + statsMap);
        }
    }
}
