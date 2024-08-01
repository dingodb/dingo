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

package io.dingodb.store.proxy.ddl;

import com.google.common.collect.Queues;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.ddl.ActionType;
import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.ddl.DdlJobEventSource;
import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.ddl.JobState;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.session.SessionUtil;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.Pair;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.service.CoordinatorService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.coordinator.CreateIdsRequest;
import io.dingodb.sdk.service.entity.coordinator.IdEpochType;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DdlHandler {

    public static final DdlHandler INSTANCE = new DdlHandler();

    private static final BlockingQueue<DdlJob> asyncJobQueue = new LinkedBlockingDeque<>(1000);

    private static final String INSERT_JOB = "insert into mysql.dingo_ddl_job(job_id, reorg, schema_ids, table_ids, job_meta, type, processing) values";

    private DdlHandler() {
        start();
    }

    public static void start() {
        new Thread(DdlHandler::limitDdlJobs).start();
    }

    public static void limitDdlJobs() {
        while (!Thread.interrupted()) {
            List<DdlJob> jobList = new ArrayList<>();
            try {
                Queues.drain(asyncJobQueue, jobList, 5, 300, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
            }
            insertDDLJobs2Table(jobList, true);
        }
    }

    public static void insertDDLJobs2Table(List<DdlJob> ddlJobList, boolean updateRawArgs) {
        if (ddlJobList.isEmpty()) {
            return;
        }
        InfoSchemaService service = InfoSchemaService.root();
        List<Long> ids = service.genGlobalIDs(ddlJobList.size());
        // jdbc insert into ddlJob to table
        // insert into dingo_ddl_job
        StringBuilder sqlBuilder = new StringBuilder(INSERT_JOB);
        String format = "(%d, %b, %s, %s, %s, %d, %b)";
        for (int i = 0; i < ddlJobList.size(); i++) {
            DdlJob job = ddlJobList.get(i);
            job.setId(ids.get(i));
            if (i != 0) {
                sqlBuilder.append(",");
            }
            job.setState(JobState.jobStateQueueing);
            byte[] meta = job.encode(updateRawArgs);
            String jobMeta = new String(meta);
            sqlBuilder.append(
                String.format(
                    format, job.getId(), job.mayNeedReorg(), Utils.quoteForSql(job.job2SchemaIDs()),
                    Utils.quoteForSql(job.job2TableIDs()), Utils.quoteForSql(jobMeta), job.getActionType().getCode(), !job.notStarted()
                )
            );
        }
        String sql = sqlBuilder.toString();
        SessionUtil.INSTANCE.exeUpdateInTxn(sql);
        asyncNotify(ddlJobList.size());
    }

    public static void asyncNotify(int size) {
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        if (env.ddlOwner.get()) {
            DdlJobEventSource ddlJobEventSource = DdlJobEventSource.ddlJobEventSource;
            forcePut(ddlJobEventSource.ownerJobQueue, size);
        } else {
            InfoSchemaService infoSchemaService = InfoSchemaService.root();
            infoSchemaService.putKvToCoordinator(DdlUtil.ADDING_DDL_JOB_CONCURRENT_KEY, "1");
        }
    }

    public static void createTableWithInfo(String schemaName, TableDefinition tableDefinition, String connId, String sql) {
        DdlJob ddlJob = createTableWithInfoJob(schemaName, tableDefinition);
        ddlJob.setConnId(connId);
        //ddlJob.setQuery(sql);
        try {
            doDdlJob(ddlJob);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage());
            throw e;
        }
    }

    public static void dropTable(SchemaInfo schemaInfo, Long tableId, String tableName, String connId) {
        DdlJob job = DdlJob.builder()
            .actionType(ActionType.ActionDropTable)
            .tableName(tableName)
            .tableId(tableId)
            .schemaId(schemaInfo.getSchemaId())
            .schemaName(schemaInfo.getName())
            .schemaState(schemaInfo.getSchemaState())
            .build();
        job.setConnId(connId);
        doDdlJob(job);
    }

    public static void createSchema(String schemaName, String connId) {
        SchemaInfo schemaInfo = SchemaInfo.builder().name(schemaName).build();
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        assert infoSchemaService != null;
        long schemaId = infoSchemaService.genSchemaId();
        schemaInfo.setSchemaId(schemaId);
        DdlJob job = DdlJob.builder()
            .actionType(ActionType.ActionCreateSchema)
            .schemaName(schemaName)
            .schemaId(schemaId).build();
        List<Object> args = new ArrayList<>();
        args.add(schemaInfo);
        job.setArgs(args);
        job.setConnId(connId);
        try {
            doDdlJob(job);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage());
            throw e;
        }
    }

    public static void dropSchema(SchemaInfo schemaInfo, String connId) {
        DdlJob job = DdlJob.builder()
            .actionType(ActionType.ActionDropSchema)
            .schemaState(schemaInfo.getSchemaState())
            .schemaName(schemaInfo.getName())
            .schemaId(schemaInfo.getSchemaId()).build();
        job.setConnId(connId);
        doDdlJob(job);
    }

    public static void truncateTable(SchemaInfo schemaInfo, Table table, String connId) {
        DdlJob job = DdlJob.builder()
            .actionType(ActionType.ActionTruncateTable)
            .schemaName(schemaInfo.getName())
            .schemaId(schemaInfo.getSchemaId())
            .tableId(table.tableId.seq)
            .tableName(table.getName())
            .schemaState(schemaInfo.getSchemaState())
            .build();
        job.setConnId(connId);
        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
        Long tableEntityId = coordinatorService.createIds(
            TsoService.getDefault().tso(),
            CreateIdsRequest.builder()
                .idEpochType(IdEpochType.ID_NEXT_TABLE).count(1)
                .build()).getIds().get(0);
        List<Object> args = new ArrayList<>();
        args.add(tableEntityId);
        job.setArgs(args);

        doDdlJob(job);
    }

    public void createIndex() {

    }

    public void dropIndex() {

    }

    public static DdlJob createTableWithInfoJob(String schemaName, TableDefinition tableDefinition) {
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        assert infoSchemaService != null;
        SchemaInfo schemaInfo = infoSchemaService.getSchema(schemaName);
        List<Object> args = new ArrayList<>();
        args.add(tableDefinition);

        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
        long tableEntityId = coordinatorService.createIds(
            TsoService.getDefault().tso(),
            CreateIdsRequest.builder()
                .idEpochType(IdEpochType.ID_NEXT_TABLE).count(1)
                .build()
        ).getIds().get(0);
        return DdlJob.builder()
            .schemaId(schemaInfo.getSchemaId())
            .schemaName(schemaInfo.getName())
            .tableName(tableDefinition.getName())
            .actionType(ActionType.ActionCreateTable)
            .state(JobState.jobStateQueueing)
            .args(args)
            .tableId(tableEntityId)
            .id(0)
            .build();
    }

    public static void doDdlJob(DdlJob job) {
        // put job to queue
        forcePut(asyncJobQueue, job);
        // get history job from history
        while (!Thread.interrupted()) {
            Pair<Boolean, String> res = historyJob(job.getId());
            if (res.getKey()) {
                return;
            } else if (res.getValue() != null) {
                throw new RuntimeException(res.getValue());
            }
            Utils.sleep(100);
        }
    }

    public static Pair<Boolean, String> historyJob(long jobId) {
        DdlJob ddlJob = getHistoryJobById(jobId);
        if (ddlJob == null) {
            return Pair.of(false, null);
        }
        if (ddlJob.getState() == JobState.jobStateSynced) {
            return Pair.of(true, null);
        }
        if (ddlJob.getError() != null) {
            return Pair.of(false, ddlJob.getError());
        }
        return Pair.of(false, null);
    }

    public static DdlJob getHistoryJobById(long jobId) {
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        assert infoSchemaService != null;
        return infoSchemaService.getHistoryDDLJob(jobId);
    }

    public static <T> void forcePut(@NonNull BlockingQueue<T> queue, T item) {
        while (true) {
            try {
                queue.put(item);
                break;
            } catch (InterruptedException ignored) {
            }
        }
    }

}
