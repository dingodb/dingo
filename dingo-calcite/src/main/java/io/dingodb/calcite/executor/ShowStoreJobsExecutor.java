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

package io.dingodb.calcite.executor;

import io.dingodb.meta.InfoSchemaService;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ShowStoreJobsExecutor extends QueryExecutor {

    public ShowStoreJobsExecutor() {
    }

    @Override
    Iterator<Object[]> getIterator() {
        List<Object[]> rows = new ArrayList<>();
        try {
            InfoSchemaService infoSchemaService = InfoSchemaService.root();
            Map<String, String> globalVarMap = infoSchemaService.getGlobalVariables();
            String jobNeedGc = globalVarMap.getOrDefault("job_need_gc", "on");
            String txnHistoryDuration = globalVarMap.getOrDefault("txn_history_duration", "0");
            String safepointTs = globalVarMap.getOrDefault("safepoint_ts", "0");
            rows.add(new Object[] {"job_need_gc", jobNeedGc});
            rows.add(new Object[] {"txn_history_duration", txnHistoryDuration});
            rows.add(new Object[] {"safepoint_ts", safepointTs});
        } catch (Exception e) {
            rows.add(new Object[] {"error", e.getMessage()});
        }
        return rows.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("variable");
        columns.add("value");
        return columns;
    }
}
