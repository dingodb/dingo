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
import io.dingodb.common.mysql.DingoErr;
import io.dingodb.common.mysql.DingoErrUtil;
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
        return doReorgWork(dc, job, tableId, replicaTable, worker, elements, BackFilling.typeAddColumnWorker);
    }

    public static Pair<Boolean, Long> doReorgWorkForDropCol(
        DdlContext dc,
        DdlJob job,
        CommonId tableId,
        TableDefinitionWithId replicaTable,
        DdlWorker worker
    ) {
        MetaElement[] elements = new MetaElement[] {
            new MetaElement(replicaTable.getTableId().getEntityId(), DdlUtil.dropColElementKey)
        };
        return doReorgWork(dc, job, tableId, replicaTable, worker, elements, BackFilling.typeDropColumnWorker);
    }

    public static DingoErr doReorgWorkForModifyCol(
        DdlContext dc,
        DdlJob job,
        CommonId tableId,
        TableDefinitionWithId replicaTable,
        DdlWorker worker
    ) {
        MetaElement[] elements = new MetaElement[] {
            new MetaElement(replicaTable.getTableId().getEntityId(), DdlUtil.modifyColElementKey)
        };
        return doReorgWorkWithStdErr(
            dc, job, tableId, replicaTable, worker, elements, BackFilling.typeModifyColumnWorker);
    }

    public static DingoErr doReorgWorkForModifyIndexCol(
        DdlContext dc,
        DdlJob job,
        CommonId tableId,
        TableDefinitionWithId replicaTable,
        DdlWorker worker
    ) {
        MetaElement[] elements = new MetaElement[] {
            new MetaElement(replicaTable.getTableId().getEntityId(), DdlUtil.modifyColElementKey)
        };
        return doReorgWorkWithStdErr(
            dc, job, tableId, replicaTable, worker, elements, BackFilling.typeModifyIndexColumnWorker);
    }

    public static Pair<Boolean, Long> doReorgWork(
        DdlContext dc,
        DdlJob job,
        CommonId tableId,
        TableDefinitionWithId replicaTable,
        DdlWorker worker,
        MetaElement[] elements,
        int reorgCode
    ) {
        // get schemaInfo
        SchemaInfo schemaInfo = (SchemaInfo) InfoSchemaService.root().getSchema(job.getSchemaId());
        Reorg reorg = Reorg.INSTANCE;
        Pair<ReorgInfo, String> reorgInfoRes = reorg.getReorgInfo(job, schemaInfo, tableId, elements, replicaTable);
        if (reorgInfoRes.getValue() != null) {
            throw DingoErrUtil.newStdErr(reorgInfoRes.getValue());
        }
        ReorgInfo reorgInfo = reorgInfoRes.getKey();
        String error = worker.runReorgJob(dc, reorgInfo,
            p -> addReplicaTable(reorgInfoRes.getKey(), reorgCode)
        );
        if (error != null) {
            throw DingoErrUtil.newStdErr(error);
        }
        return Pair.of(true, 0L);
    }

    public static DingoErr doReorgWorkWithStdErr(
        DdlContext dc,
        DdlJob job,
        CommonId tableId,
        TableDefinitionWithId replicaTable,
        DdlWorker worker,
        MetaElement[] elements,
        int reorgCode
    ) {
        // get schemaInfo
        SchemaInfo schemaInfo = (SchemaInfo) InfoSchemaService.root().getSchema(job.getSchemaId());
        Reorg reorg = Reorg.INSTANCE;
        Pair<ReorgInfo, String> reorgInfoRes = reorg.getReorgInfo(job, schemaInfo, tableId, elements, replicaTable);
        if (reorgInfoRes.getValue() != null) {
            throw DingoErrUtil.newStdErr(reorgInfoRes.getValue());
        }
        ReorgInfo reorgInfo = reorgInfoRes.getKey();
        DingoErr error = worker.runReorgJobWithStdErr(dc, reorgInfo,
            p -> addReplicaTableWithEx(reorgInfoRes.getKey(), reorgCode)
        );
        if (error != null) {
            return error;
        }
        return DingoErrUtil.normal();
    }

    public static String addReplicaTable(ReorgInfo reorgInfo, int fillType) {
        try {
            return BackFilling.writePhysicalTableRecord(fillType, reorgInfo);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            return e.getMessage();
        }
    }

    public static String addReplicaTableWithEx(ReorgInfo reorgInfo, int fillType) {
        return BackFilling.writePhysicalTableRecord(fillType, reorgInfo);
    }

}
