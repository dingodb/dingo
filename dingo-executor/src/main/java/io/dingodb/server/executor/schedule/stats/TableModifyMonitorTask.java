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
import io.dingodb.common.CommonId;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class TableModifyMonitorTask extends StatsOperator implements Runnable {

    private static final BigDecimal MODIFY_COMMIT_RATE = new BigDecimal(0.3);

    @Override
    public void run() {
        // lookup schema -> table commits
        // calculate last commit is the threshold exceeded
        // insert into analyze task

        MetaService metaService = MetaService.root();
        List<Object[]> analyzeTaskList = new ArrayList<>();
        Map<CommonId, Long> commitCountMap = metaService.getTableCommitCount();
        Map<String, MetaService> subMetaServiceMap = metaService.getSubMetaServices();
        subMetaServiceMap.forEach((key, subMetaService) -> {
            if (key.equalsIgnoreCase("mysql")
                || key.equalsIgnoreCase("information_schema")) {
                return;
            }
            Set<Table> tables = subMetaService.getTables();
            tables.forEach(t -> {
                CommonId commonId = t.tableId;
                Long commitCount = 0L;
                if (commitCountMap.containsKey(commonId)) {
                    commitCount = commitCountMap.get(commonId);
                }
                Double totalCount = subMetaService.getTableStatistic(t.name).getRowCount();
                if (autoAnalyzeTriggerPolicy(key, t.name, commitCount)) {
                    if (totalCount > 0 && commitCount > totalCount) {
                        commitCount = totalCount.longValue();
                    }
                    analyzeTaskList.add(generateAnalyzeTask(key, t.name, totalCount.longValue(), commitCount));
                }
            });
        });

        if (analyzeTaskList.size() > 0) {
            upsert(analyzeTaskStore, analyzeTaskCodec, analyzeTaskList);
        }
    }

    /**
     * auto analyze trigger policy.
     * 1. count > 1000
     * 2. (this table commit - last table commit) > 1000
     * @param schemaName schema custom
     * @param tableName table
     * @param commitCount update,delete,insert
     * @return auto analyze flag
     */
    private boolean autoAnalyzeTriggerPolicy(String schemaName, String tableName, long commitCount) {
        long lastCommit = 0;
        long processRows = 0;
        try {
            Object[] values = get(analyzeTaskStore, analyzeTaskCodec, getAnalyzeTaskKeys(schemaName, tableName));
            if (values != null) {
                if (values[9] != null) {
                    lastCommit = (long) values[9];
                }
                if (values[3] != null) {
                    processRows = (long) values[3];
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        commitCount -= lastCommit;
        if (processRows == 0 && commitCount > 1000) {
            return true;
        } else if (processRows > 1000) {
            BigDecimal modify = new BigDecimal(commitCount);
            BigDecimal count = new BigDecimal(processRows);
            BigDecimal rate = modify.divide(count, RoundingMode.HALF_UP);
            return rate.compareTo(MODIFY_COMMIT_RATE) > 0;
        }
        return false;
    }

}
