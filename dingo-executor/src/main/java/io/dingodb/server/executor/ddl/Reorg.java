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
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.ddl.ActionType;
import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.ddl.MetaElement;
import io.dingodb.common.ddl.ReorgInfo;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.util.Pair;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.store.proxy.mapper.Mapper;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@Slf4j
public class Reorg {
    public static final Reorg INSTANCE = new Reorg();
    private Reorg() {

    }

    public Pair<ReorgInfo, String> getReorgInfo(
        DdlJob job,
        SchemaInfo schemaInfo,
        CommonId tableId,
        MetaElement[] elements,
        TableDefinitionWithId index
    ) {
        ReorgInfo info = ReorgInfo.builder()
            .schemaInfo(schemaInfo)
            .tableId(tableId)
            .ddlJob(job)
            .first(true)
            .elements(elements)
            .indexId(Mapper.MAPPER.idFrom(index.getTableId()))
            .build();
        long startTs = TsoService.getDefault().tso();
        job.setSnapshotVer(startTs);
        return Pair.of(info, null);
    }

    public String isReorgRunnable(long jobId) {
        DdlContext dc = DdlContext.INSTANCE;
        if (dc.getReorgCtx1(jobId).isReorgCanceled()) {
            return "ErrCancelledDDLJob";
        }
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        if (!env.ddlOwner.get()) {
            LogUtils.info(log, "[ddl] DDL is not the DDL owner");
            return "ErrNotOwner";
        }
        return null;
    }

}
