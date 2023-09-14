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
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.MetaService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class TableModifyMonitorTask extends StatsOperator implements Runnable {

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
            Map<String, TableDefinition> tableDefinitionMap = subMetaService.getTableDefinitions();
            tableDefinitionMap.values().forEach(t -> {
                CommonId commonId = subMetaService.getTableId(t.getName());
                Long commitCount = 0L;
                if (commitCountMap.containsKey(commonId)) {
                    commitCount = commitCountMap.get(commonId);
                }
                Double totalCount = subMetaService.getTableStatistic(t.getName()).getRowCount();
                if (autoAnalyzeTriggerPolicy(key, t.getName(), commitCount, totalCount)) {
                    if (commitCount > totalCount) {
                        commitCount = totalCount.longValue();
                    }
                    analyzeTaskList.add(generateAnalyzeTask(key, t.getName(),
                        totalCount.longValue(), commitCount));
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
     * 2. (this table commit - last table commit)/totalCount > 0.5
     * @param schemaName schema custom
     * @param tableName table
     * @param commitCount update,delete,insert
     * @param totalCount table row count
     * @return auto analyze flag
     */
    private boolean autoAnalyzeTriggerPolicy(String schemaName, String tableName, long commitCount,
                                             Double totalCount) {
        long lastCommit = 0;
        try {
            Object[] values = get(analyzeTaskStore, analyzeTaskCodec, getAnalyzeTaskKeys(schemaName, tableName));
            if (values != null) {
                lastCommit = (long) values[9];
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        commitCount -= lastCommit;
        if (totalCount > 1000) {
            double modifyRate = commitCount / totalCount;
            if (modifyRate > 0.5) {
                return true;
            }
        }
        return false;
    }

}
