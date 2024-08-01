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
import io.dingodb.common.ddl.JobState;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.Pair;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public final class TableUtil {

    private TableUtil() {
    }

    public static Pair<TableDefinition, String> createTable(DdlJob ddlJob) {
        long schemaId = ddlJob.getSchemaId();
        TableDefinition tableInfo = (TableDefinition) ddlJob.getArgs().get(0);
        tableInfo.setSchemaState(SchemaState.SCHEMA_NONE);
        long tableId = ddlJob.getTableId();
        tableInfo.setPrepareTableId(tableId);

        InfoSchemaService service = InfoSchemaService.root();
        assert service != null;
        Object tabObj = service.getTable(schemaId, tableInfo.getName());
        if (tabObj != null) {
            ddlJob.setState(JobState.jobStateCancelled);
            return Pair.of(null, "table has exist");
        }
        if (tableInfo.getSchemaState() == SchemaState.SCHEMA_NONE) {
            tableInfo.setSchemaState(SchemaState.SCHEMA_PUBLIC);
            MetaService metaService = MetaService.root();
            MetaService subMs = metaService.getSubMetaService(ddlJob.getSchemaName());
            List<TableDefinition> indices = tableInfo.getIndices();
            if (indices != null) {
                indices.forEach(index -> index.setSchemaState(SchemaState.SCHEMA_PUBLIC));
            }
            try {
                assert indices != null;
                subMs.createTables(tableInfo, indices);
                return Pair.of(tableInfo, null);
            } catch (Exception e) {
                subMs.rollbackCreateTable(tableInfo, indices);
                LogUtils.error(log, e.getMessage(), e);
                ddlJob.setState(JobState.jobStateCancelled);
                String error = "[ddl] meta service createTables error";
                return Pair.of(null, error);
            }
        }
        return Pair.of(tableInfo, "ErrInvalidDDLState");
    }

    public static Pair<Long, String> updateVersionAndTableInfos(DdlContext dc,
                                                                DdlJob job,
                                                                TableDefinitionWithId tableInfo,
                                                                boolean shouldUpdateVer) {
        Long version = 0L;
        if (shouldUpdateVer) {
            Pair<Long, String> res= DdlWorker.updateSchemaVersion(dc, job);
            if (res.getValue() != null) {
                return res;
            }
            version = res.getKey();
        }
        updateTable(job.getSchemaId(), tableInfo);
        return Pair.of(version, null);
    }

    public static void updateTable(long schemaId, TableDefinitionWithId tableInfo) {
        // to set startTs
        InfoSchemaService infoSchemaService =InfoSchemaService.root();
        assert infoSchemaService != null;
        infoSchemaService.updateTable(schemaId, tableInfo);
    }

    public static Pair<TableDefinitionWithId, String> getTableInfoAndCancelFaultJob(DdlJob ddlJob, long schemaId) {
        Pair<TableDefinitionWithId, String> res = DdlWorker.checkTableExistAndCancelNonExistJob(ddlJob, schemaId);
        if (res.getValue() != null || res.getKey() == null) {
            return res;
        }
        if (res.getKey().getTableDefinition().getSchemaState()
              == io.dingodb.sdk.service.entity.common.SchemaState.SCHEMA_PUBLIC) {
            ddlJob.setState(JobState.jobStateCancelled);
            return Pair.of(null, "table is not in public");
        }
        return res;
    }
}
