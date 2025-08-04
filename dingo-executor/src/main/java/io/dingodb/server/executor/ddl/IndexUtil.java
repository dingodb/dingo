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
import io.dingodb.common.ddl.DdlReorgMeta;
import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.ddl.MetaElement;
import io.dingodb.common.ddl.ReorgInfo;
import io.dingodb.common.ddl.ReorgType;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.session.Session;
import io.dingodb.common.session.SessionUtil;
import io.dingodb.common.util.Pair;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static io.dingodb.server.executor.ddl.BackFilling.typeAddIndexWorker;

@Slf4j
public final class IndexUtil {
    public static final IndexUtil INSTANCE = new IndexUtil();

    private IndexUtil() {
    }

    public static TableDefinitionWithId getIndexWithId(Table table, String indexName) {
        long indexId = table.getIndexes().stream()
            .filter(s -> s.getName().equalsIgnoreCase(indexName))
            .map(s -> s.getTableId().seq)
            .findFirst().orElse(0L);

        return (TableDefinitionWithId) InfoSchemaService.root().getIndex(table.tableId.seq, indexId);
    }

    public Pair<Boolean, Long> doReorgWorkForCreateIndex(
        DdlContext dc,
        DdlJob job,
        DdlWorker worker,
        CommonId tableId,
        TableDefinitionWithId index
    ) {
        if (job.getReorgMeta().getReorgType() == ReorgType.ReorgTypeNone) {
            return runReorgJobAndHandleErr(dc, job, worker, tableId, index, typeAddIndexWorker, null);
        }
        // not support;
        return null;
    }

    public Pair<Boolean, Long> runReorgJobAndHandleErr(
        DdlContext dc,
        DdlJob job,
        DdlWorker worker,
        CommonId tableId,
        TableDefinitionWithId index,
        int workType,
        List<Long> regionIdList
    ) {
        MetaElement[] elements = new MetaElement[] {new MetaElement(index.getTableId().getEntityId(),
            DdlUtil.indexElementKey)};
        Session session = SessionUtil.INSTANCE.getSession();
        try {
            // get schemaInfo
            SchemaInfo schemaInfo = (SchemaInfo) InfoSchemaService.root().getSchema(job.getSchemaId());
            if (schemaInfo == null) {
                LogUtils.error(log, "reorg get schemaInfo is null, job:{}", job);
                return Pair.of(false, 0L);
            }
            Reorg reorg = Reorg.INSTANCE;
            Pair<ReorgInfo, String> reorgInfoRes = reorg.getReorgInfo(job, schemaInfo, tableId, elements, index);
            if (reorgInfoRes.getValue() != null) {
                throw new RuntimeException(reorgInfoRes.getValue());
            }
            ReorgInfo reorgInfo = reorgInfoRes.getKey();
            if (regionIdList != null) {
                reorgInfo.setRegionIdList(regionIdList);
            }
            String error = worker.runReorgJob(dc, reorgInfo,
                p -> opPhysicalTableIndex(reorgInfoRes.getKey(), workType)
            );
            if (error != null) {
                LogUtils.warn(log, "[ddl] run add index job failed, convert job to rollback, jobId:{}, "
                    + "error:{}", job.getId(), error);
                Pair<Long, String> res = RollingBackUtil.convertAddIdxJob2RollbackJob(dc, job, index);
                if (res.getValue() != null) {
                    error = res.getValue();
                }
                throw new RuntimeException(error);
            }
            return Pair.of(true, 0L);
        } finally {
            SessionUtil.INSTANCE.closeSession(session);
        }
    }

    public static void pickBackFillType(DdlJob ddlJob) {
        if (ddlJob.getReorgMeta() == null) {
            ddlJob.setReorgMeta(new DdlReorgMeta(0, 0, ReorgType.ReorgTypeNone));
        }
    }

    public static String opPhysicalTableIndex(ReorgInfo reorgInfo, int workType) {
        if (reorgInfo.isMergingTmpIndex()) {
            // optimize to implement
            return null;
        }
        LogUtils.info(log, "[ddl] start to op table index, jobId:{}", reorgInfo.getDdlJob().getId());
        return BackFilling.writePhysicalTableRecord(workType, reorgInfo);
    }

}
