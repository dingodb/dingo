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

import io.dingodb.common.CommonId;
import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.ddl.MetaElement;
import io.dingodb.common.ddl.ReorgInfo;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.session.Session;
import io.dingodb.common.session.SessionUtil;
import io.dingodb.common.util.Pair;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.sdk.service.entity.common.SchemaState;
import io.dingodb.sdk.service.entity.meta.ColumnDefinition;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public final class DdlColumn {
    private DdlColumn() {
    }

    public static void generateOriginDefaultValue(ColumnDefinition columnDef) {
        Object odVal = columnDef.getDefaultVal();
        if (odVal == null && !columnDef.isNullable()) {
            switch (columnDef.getSqlType()) {
                case "ENUM":
                    break;
                default:
                    columnDef.setDefaultVal("0");
            }
        }
    }

    public static void setIndicesState(List<TableDefinitionWithId> indexInfoList, SchemaState schemaState) {
        if (indexInfoList == null) {
            return;
        }
        indexInfoList.forEach(indexInfo -> {
            indexInfo.getTableDefinition().setSchemaState(schemaState);
            TableUtil.updateIndex(indexInfo);
        });
    }

    public static Pair<Boolean, Long> doReorgWorkForAddCol(
        DdlContext dc,
        DdlJob job,
        CommonId tableId,
        TableDefinitionWithId replicaTable,
        DdlWorker worker
    ) {
        MetaElement[] elements = new MetaElement[] {
            new MetaElement(replicaTable.getTableId().getEntityId(), DdlUtil.addColElementKey)
        };
        Session session = SessionUtil.INSTANCE.getSession();
        try {
            // get schemaInfo
            SchemaInfo schemaInfo = (SchemaInfo) InfoSchemaService.root().getSchema(job.getSchemaId());
            if (schemaInfo == null) {
                return Pair.of(false, 0L);
            }
            Reorg reorg = Reorg.INSTANCE;
            Pair<ReorgInfo, String> reorgInfoRes = reorg.getReorgInfo(job, schemaInfo, tableId, elements, replicaTable);
            if (reorgInfoRes.getValue() != null) {
                throw new RuntimeException(reorgInfoRes.getValue());
            }
            ReorgInfo reorgInfo = reorgInfoRes.getKey();
            //if (reorgInfo.isFirst()) {
            //    return Pair.of(false, 0L);
            //}
            String error = worker.runReorgJob(dc, reorgInfo,
                p -> addReplicaTable(reorgInfoRes.getKey())
            );
            if (error != null) {
                if ("ErrWaitReorgTimeout".equalsIgnoreCase(error)) {
                    return Pair.of(false, 0L);
                }
                if ("ErrKeyExists".equalsIgnoreCase(error)
                    || "ErrCancelledDDLJob".equalsIgnoreCase(error)
                    || "ErrCantDecodeRecord".equalsIgnoreCase(error))
                {
                    LogUtils.warn(log, "[ddl] run add index job failed, convert job to rollback, jobId:{}, error:{}", job.getId(), error);
                    //Pair<Long, String> res = RollingBackUtil.convertAddIdxJob2RollbackJob(dc, job, replicaTable);
                    //if (res.getValue() != null) {
                    //    error = res.getValue();
                    //}
                    String error1 = JobTableUtil.removeDDLReorgHandle(session, job.getId(), reorgInfo.getElements());
                    if (error1 != null) {
                        LogUtils.warn(log, "[ddl] run add index job failed, convert job to rollback, RemoveDDLReorgHandle failed, jobId:{}, error:{}", job.getId(), error1);
                    }
                }
                throw new RuntimeException(error);
            }
            return Pair.of(true, 0L);
        } finally {
            SessionUtil.INSTANCE.closeSession(session);
        }
    }

    public static String addReplicaTable(ReorgInfo reorgInfo) {
        try {
            return BackFilling.writePhysicalTableRecord(BackFilling.typeAddColumnWorker, reorgInfo);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            return "reorgAddColumnError:" + e.getMessage();
        }
    }

}
