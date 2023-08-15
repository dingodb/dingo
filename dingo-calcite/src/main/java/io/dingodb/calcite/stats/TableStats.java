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

import java.util.ArrayList;
import java.util.List;

public class TableStats {
    private String schemaName;
    private String tableName;
    private final List<CountMinSketch> countMinSketchList;
    private final List<Histogram> histogramList;
    private final List<StatsNormal> statsNormalList;

    public List<CountMinSketch> getCountMinSketchList() {
        return countMinSketchList;
    }

    public List<Histogram> getHistogramList() {
        return histogramList;
    }

    public List<StatsNormal> getStatsNormalList() {
        return statsNormalList;
    }

    public TableStats(
                      List<CountMinSketch> countMinSketchList,
                      List<Histogram> histogramList,
                      List<StatsNormal> statsNormalList) {
        this.countMinSketchList = countMinSketchList;
        this.histogramList = histogramList;
        this.statsNormalList = statsNormalList;
        if (countMinSketchList.size() > 0) {
            this.schemaName = countMinSketchList.get(0).getSchemaName();
            this.tableName = countMinSketchList.get(0).getTableName();
        } else if (histogramList.size() > 0) {
            this.schemaName = histogramList.get(0).getSchemaName();
            this.tableName = histogramList.get(0).getTableName();
        }
    }

    public TableStats(String schemaName, String tableName) {
        this.histogramList = new ArrayList<>();
        this.countMinSketchList = new ArrayList<>();
        this.statsNormalList = new ArrayList<>();
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public void setNdv() {
        statsNormalList.forEach(StatsNormal::setNdv);
    }

    public static void mergeStats(List<TableStats> tableStatsList) {
        tableStatsList.forEach(TableStats::setNdv);
        // one region do not need merge
        if (tableStatsList.size() == 1) {
            List<StatsNormal> statsNormalList;
            if ((statsNormalList = tableStatsList.get(0).statsNormalList) != null) {
                statsNormalList.forEach(StatsNormal::calculateAvgColSize);
            }
            return;
        }
        // merge histogram
        // get first col histogram
        if (tableStatsList.get(0).histogramList.size() > 0) {
            List<Histogram> firstHistogramList = tableStatsList.get(0).histogramList;
            for (Histogram colHistogram : firstHistogramList) {
                for (int i = 1; i < tableStatsList.size(); i++) {
                    Histogram that = tableStatsList.get(i).getHistogramList().get(i);
                    colHistogram.merge(that);
                }
            }
        }
        // merge count-min-sketch
        // get first col cmsketch
        if (tableStatsList.get(0).countMinSketchList.size() > 0) {
            List<CountMinSketch> firstCmSketchList = tableStatsList.get(0).countMinSketchList;
            for (CountMinSketch countMinSketch : firstCmSketchList) {
                for (int i = 1; i < tableStatsList.size(); i++) {
                    CountMinSketch that = tableStatsList.get(i).countMinSketchList.get(i);
                    countMinSketch.merge(that);
                }
            }
        }
        // merge stats-normal
        if (tableStatsList.get(0).statsNormalList.size() > 0) {
            List<StatsNormal> firstStatsNormalList = tableStatsList.get(0).statsNormalList;
            for (StatsNormal statsNormal : firstStatsNormalList) {
                for (int i = 1; i < tableStatsList.size(); i++) {
                    StatsNormal that = tableStatsList.get(i).statsNormalList.get(i);
                    statsNormal.merge(that);
                }
                statsNormal.calculateAvgColSize();
            }
        }
    }

    public String getIdentifier() {
        return schemaName.toUpperCase() + "." + tableName.toUpperCase();
    }
}
