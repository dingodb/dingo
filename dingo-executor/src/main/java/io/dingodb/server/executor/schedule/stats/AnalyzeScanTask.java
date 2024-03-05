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

package io.dingodb.server.executor.schedule.stats;

import io.dingodb.calcite.stats.StatsOperator;
import io.dingodb.calcite.stats.StatsTaskState;
import io.dingodb.calcite.stats.task.AnalyzeTask;
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.NavigableMap;

@Slf4j
public class AnalyzeScanTask extends StatsOperator implements Runnable {
    LinkedRunner linkedRunner = new LinkedRunner("analyze");

    @Override
    public void run() {
        // query analyze task
        // add linkedRunner
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges
            = metaService.getRangeDistribution(analyzeTaskTblId);
        RangeDistribution rangeDistribution = ranges.values().stream().findFirst().orElse(null);
        if (rangeDistribution == null) {
            return;
        }
        List<Object[]> analyzeTaskList = scan(analyzeTaskStore, analyzeTaskCodec, rangeDistribution);

        analyzeTaskList.forEach(v -> {
            String state = (String) v[6];
            if (!state.equals(StatsTaskState.PENDING.getState())) {
                return;
            }
            AnalyzeTask analyzeTask = AnalyzeTask.builder()
                .schemaName((String) v[0])
                .tableName((String) v[1])
                .build();
            if (log.isDebugEnabled()) {
                log.debug("analyze table task add task queue, detail:" + analyzeTask.toString());
            }
            linkedRunner.follow(analyzeTask);
        });
    }
}
