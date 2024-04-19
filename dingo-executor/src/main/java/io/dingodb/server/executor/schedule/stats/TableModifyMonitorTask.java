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
import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.store.KeyValue;
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
     * @param schemaName schema custom
     * @param tableName table
     * @param commitCount update,delete,insert
     * @return auto analyze flag
     */
    protected boolean autoAnalyzeTriggerPolicy(String schemaName, String tableName, long commitCount) {
        long processRows = 0;
        KeyValue old = null;
        Object[] oldValues = null;
        try {
            Object[] keys = getAnalyzeTaskKeys(schemaName, tableName);
            old = analyzeTaskStore.get(analyzeTaskCodec.encodeKey(keys));
            if (!(old.getValue() == null || old.getValue().length == 0)) {
                oldValues = analyzeTaskCodec.decode(old);
            }
            if (oldValues != null) {
                if (oldValues[3] != null) {
                    processRows = (long) oldValues[3];
                }
                if (oldValues[6] != null) {
                    String state = (String) oldValues[6];
                    if (StatsTaskState.PENDING.getState().equalsIgnoreCase(state)
                     || StatsTaskState.RUNNING.getState().equalsIgnoreCase(state)) {
                        return false;
                    } else if (StatsTaskState.INIT.getState().equalsIgnoreCase(state)) {
                        if (oldValues[9] != null) {
                            long modify = (long) oldValues[9];
                            commitCount += modify;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        boolean res = false;
        if (processRows == 0 && commitCount > 10000) {
            res = true;
        } else if (commitCount > 10000 && processRows > 10000) {
            BigDecimal modify = new BigDecimal(commitCount);
            BigDecimal count = new BigDecimal(processRows);
            BigDecimal rate = modify.divide(count, 2, RoundingMode.HALF_UP);
            res = rate.compareTo(MODIFY_COMMIT_RATE) > 0;
        }
        if (!res && oldValues != null) {
            Object[] row = generateAnalyzeTask(schemaName, tableName, 0, commitCount);
            row[6] = StatsTaskState.INIT.getState();
            mergeAnalyzeRecord(old, oldValues, row);
        } else {
            if (!res) {
                return false;
            }
            Object[] row = generateAnalyzeTask(schemaName, tableName, 0, commitCount);
            LogUtils.info(log, "{}.{} auto analyze start, modify:{}",
                schemaName, tableName, commitCount);
            if (oldValues == null) {
                analyzeTaskStore.insert(analyzeTaskCodec.encode(row));
            } else {
                mergeAnalyzeRecord(old, oldValues, row);
            }
        }
        return res;
    }

    private static void mergeAnalyzeRecord(KeyValue old, Object[] oldValues, Object[] row) {
        row[11] = oldValues[11];
        row[12] = oldValues[12];
        row[13] = oldValues[13];
        row[14] = oldValues[14];
        row[3] = oldValues[3];
        row[2] = oldValues[2];
        String state = null;
        if (oldValues[6] != null) {
            state = (String) oldValues[6];
        }
        long rowsCount = (long) oldValues[3];
        if (rowsCount > 500000 && StatsTaskState.PENDING.getState().equalsIgnoreCase(state)) {
            return;
        }
        analyzeTaskStore.update(analyzeTaskCodec.encode(row), old);
    }

}
