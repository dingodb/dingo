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

package io.dingodb.server.executor.ddl;

import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.ddl.JobState;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.util.Pair;
import io.dingodb.sdk.service.entity.common.SchemaState;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class DdlRollBack {

    private DdlRollBack() {
    }

    public static Pair<Long, String> convertJob2RollbackJob(DdlWorker worker, DdlContext dc, DdlJob job) {
        String error = null;
        Long ver = 0L;
        Pair<Long, String> res = null;
        switch (job.getActionType()) {
            case ActionAddIndex:
                break;
            case ActionDropIndex:
                break;
            case ActionDropSchema:
                error = rollingbackDropSchema(job);
                break;
            case ActionDropTable:
                error = rollingbackDropTableOrView(job);
                break;
            case ActionTruncateTable:
                res = rollingbackTruncateTable(job);
                ver = res.getKey();
                error = res.getValue();
                break;
            default:
                job.setState(JobState.jobStateCancelled);
                error = "ErrCancelledDDLJob";
        }
        if (error != null)  {
            if (job.getError() == null) {
                job.setError(error);
            }
            job.addErrorCount(1);
            if ("ErrCancelledDDLJob".equals(error)) {
                if (!"ErrCancelledDDLJob".equals(job.getError())) {
                    job.setError(job.getError());
                }
            } else {
                if (job.getErrorCount() > DdlUtil.errorCountLimit) {
                    job.setState(JobState.jobStateCancelled);
                    job.setError("[ddl] rollback DDL job error count exceed the limit");
                }
            }

            if (job.getState() != JobState.jobStateRollingback && job.getState() != JobState.jobStateCancelled) {
                LogUtils.error(log, "[ddl] run DDL job failed");
            } else {
                LogUtils.info(log, "[ddl] the DDL job is cancelled normally");
                return Pair.of(ver, null);
            }
        }
        return res;
    }

    public static String rollingbackDropTableOrView(DdlJob ddlJob) {
        Pair<TableDefinitionWithId, String> res = DdlWorker.checkTableExistAndCancelNonExistJob(ddlJob, ddlJob.getSchemaId());
        if (res.getValue() != null && res.getKey() == null) {
            return res.getValue();
        }
        if (res.getKey().getTableDefinition().getSchemaState() == SchemaState.SCHEMA_PUBLIC) {
            ddlJob.setState(JobState.jobStateCancelled);
            return "ErrCancelledDDLJob";
        }
        ddlJob.setState(JobState.jobStateRunning);
        return null;
    }

    public static String rollingbackDropSchema(DdlJob ddlJob) {
        Pair<SchemaInfo, String> res = DdlWorker.checkSchemaExistAndCancelNotExistJob(ddlJob);
        if (res.getValue() != null) {
            return res.getValue();
        }
        if (res.getKey().getSchemaState() == io.dingodb.common.meta.SchemaState.SCHEMA_PUBLIC) {
            ddlJob.setState(JobState.jobStateCancelled);
            return "ErrCancelledDDLJob";
        }
        ddlJob.setState(JobState.jobStateRunning);
        return null;
    }

    public static Pair<Long, String> rollingbackTruncateTable(DdlJob job) {
        Pair<TableDefinitionWithId, String> tableRes
            = TableUtil.getTableInfoAndCancelFaultJob(job, job.getSchemaId());
        if (tableRes.getValue() != null && tableRes.getKey() == null) {
            return Pair.of(0L, tableRes.getValue());
        }
        return cancelOnlyNotHandledJob(job, io.dingodb.common.meta.SchemaState.SCHEMA_NONE);
    }

    public static Pair<Long, String> cancelOnlyNotHandledJob(DdlJob job, io.dingodb.common.meta.SchemaState schemaState) {
        if (job.getSchemaState() == schemaState) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, "ErrCancelledDDLJob");
        }
        job.setState(JobState.jobStateRunning);
        return Pair.of(0L, null);
    }
}
