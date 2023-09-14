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

package io.dingodb.calcite.operation;

import io.dingodb.calcite.grammar.ddl.SqlAnalyze;
import io.dingodb.calcite.stats.StatsOperator;
import io.dingodb.calcite.stats.task.AnalyzeTask;
import io.dingodb.meta.MetaService;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class AnalyzeTableOperation extends StatsOperator implements DdlOperation {

    String tableName;
    String schemaName;

    List<String> columnList;

    private int cmSketchHeight;
    private int cmSketchWidth;
    private Integer bucketCount;
    private long samples;
    private float sampleRate;

    private Long totalCount;

    public AnalyzeTableOperation(SqlAnalyze sqlAnalyze) {
        this.schemaName = sqlAnalyze.getSchemaName();
        this.tableName = sqlAnalyze.getTableName();
        MetaService metaService = MetaService.root().getSubMetaService(schemaName);
        this.totalCount = metaService.getTableStatistic(tableName).getRowCount().longValue();
        this.columnList = sqlAnalyze.getColumns();
        this.cmSketchHeight = sqlAnalyze.getCmSketchHeight();
        this.cmSketchWidth = sqlAnalyze.getCmSketchWidth();
        if (cmSketchHeight == 0 && cmSketchWidth == 0) {
            this.cmSketchWidth = 10000;
            this.cmSketchHeight = 5;
        }
        this.bucketCount = sqlAnalyze.getBuckets();
        if (bucketCount == null || bucketCount == 0) {
            bucketCount = 254;
        }
        this.samples = sqlAnalyze.getSamples();
        this.sampleRate = sqlAnalyze.getSampleRate();
    }

    @Override
    public void execute() {
        AnalyzeTask analyzeTask = AnalyzeTask.builder()
            .samples(samples)
            .schemaName(schemaName)
            .tableName(tableName.toUpperCase())
            .columnList(columnList)
            .cmSketchHeight(cmSketchHeight)
            .cmSketchWidth(cmSketchWidth)
            .bucketCount(bucketCount)
            .sampleRate(sampleRate)
            .totalCount(totalCount)
            .build();
        analyzeTask.run();
    }

}
