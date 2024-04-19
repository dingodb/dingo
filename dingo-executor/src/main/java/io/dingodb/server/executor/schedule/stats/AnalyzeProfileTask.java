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

import io.dingodb.common.profile.AnalyzeEvent;
import io.dingodb.common.profile.StmtSummaryMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AnalyzeProfileTask extends TableModifyMonitorTask implements Runnable {

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            AnalyzeEvent analyzeEvent = StmtSummaryMap.getAnalyzeEvent();
            String schemaName = analyzeEvent.getSchemaName();
            String tableName = analyzeEvent.getTableName();
            try {
                autoAnalyzeTriggerPolicy(schemaName, tableName, analyzeEvent.getModify());
            } catch (Exception ignored) {

            }
        }
    }
}
