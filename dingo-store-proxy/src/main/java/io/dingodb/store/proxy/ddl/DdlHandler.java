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

import io.dingodb.common.ddl.ActionType;
import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.ddl.DdlJobEventSource;
import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.ddl.JobState;
import io.dingodb.common.ddl.ModifyingColInfo;
import io.dingodb.common.ddl.RecoverInfo;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.mysql.DingoErr;
import io.dingodb.common.mysql.DingoErrUtil;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.sequence.SequenceDefinition;
import io.dingodb.common.session.SessionUtil;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.Pair;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.service.CoordinatorService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.entity.coordinator.CreateIdsRequest;
import io.dingodb.sdk.service.entity.coordinator.IdEpochType;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

@Slf4j
public class DdlHandler {

    public static final DdlHandler INSTANCE = new DdlHandler();
    private static final Map<Long, String> insertFailedJobIdList = new ConcurrentHashMap<>();
    private static final BlockingQueue<DdlJob> asyncJobQueue = new LinkedBlockingDeque<>(1000);

    private static final String INSERT_JOB = "insert into mysql.dingo_ddl_job(job_id, reorg, schema_ids, table_ids,"
        + " job_meta, type, processing) values";

    public DdlHandler() {
        start();
    }

    public static void start() {
        new Thread(DdlHandler::limitDdlJobs).start();
    }

    public static void limitDdlJobs() {
        while (!Thread.interrupted()) {
            DdlJob ddlJob = Utils.forceTake(asyncJobQueue);
            try {
                insertDDLJobs2Table(ddlJob, true);
            } catch (Exception e) {
                LogUtils.error(log, "[ddl] insert ddl into table error", e);
            }
        }
        LogUtils.error(log, "[ddl] limitDdlJobs exit");
        limitDdlJobs();
    }

    public static void insertDDLJobs2Table(DdlJob job, boolean updateRawArgs) {
        if (job == null) {
            return;
        }
        InfoSchemaService service = InfoSchemaService.root();
        List<Long> ids = service.genGlobalIDs(1);
        // jdbc insert into ddlJob to table
        // insert into dingo_ddl_job
        long jobId;
        job.setId(ids.get(0));
        jobId = job.getId();
        job.setState(JobState.jobStateQueueing);
        byte[] meta = job.encode(updateRawArgs);
        String jobMeta = new String(meta);
        String format = "(%d, %b, %s, %s, %s, %d, %b)";
        String sql = INSERT_JOB + String.format(
                format, job.getId(), job.mayNeedReorg(), Utils.quoteForSql(job.job2SchemaIDs()),
                Utils.quoteForSql(job.job2TableIDs()), Utils.quoteForSql(jobMeta), job.getActionType().getCode(),
                !job.notStarted()
            );
        String error = SessionUtil.INSTANCE.exeUpdateInTxn(sql);
        if (error != null) {
            LogUtils.error(log, "[ddl-error] insert ddl to table,sql:{}", sql);
            checkDdlContinue();
            if (!insertFailedJobIdList.containsKey(jobId)) {
                insertFailedJobIdList.put(jobId, error);
            }
            return;
        }
        LogUtils.info(log, "insert job 2 table,jobId:{}", jobId);
        asyncNotify(1L, jobId);
    }

    public static void checkDdlContinue() {
        if (insertFailedJobIdList.size() > 1000) {
            Utils.sleep(1000);
            checkDdlContinue();
        }
    }

    public static void asyncNotify(Long size, long jobId) {
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        if (env.ddlOwner.get()) {
            DdlJobEventSource ddlJobEventSource = DdlJobEventSource.ddlJobEventSource;
            DdlJobEventSource.forcePut(ddlJobEventSource.ownerJobQueue, size);
        } else {
            InfoSchemaService infoSchemaService = InfoSchemaService.root();
            infoSchemaService.putKvToCoordinator(DdlUtil.ADDING_DDL_JOB_CONCURRENT_KEY, String.valueOf(jobId));
        }
    }


    public static void asyncNotify(long size) {
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        if (env.ddlOwner.get()) {
            DdlJobEventSource ddlJobEventSource = DdlJobEventSource.ddlJobEventSource;
            DdlJobEventSource.forcePut(ddlJobEventSource.ownerJobQueue, size);
        } else {
            InfoSchemaService infoSchemaService = InfoSchemaService.root();
            infoSchemaService.putKvToCoordinator(DdlUtil.ADDING_DDL_JOB_CONCURRENT_KEY, "1");
        }
    }

    public void createTableWithInfo(
        String schemaName,
        TableDefinition tableDefinition,
        String connId,
        String sql
    ) {
        DdlJob ddlJob = createTableWithInfoJob(schemaName, tableDefinition);
        ddlJob.setConnId(connId);
        //ddlJob.setQuery(sql);
        doDdlJob(ddlJob);
    }

    public void createViewWithInfo(
        String schemaName,
        TableDefinition tableDefinition,
        String connId,
        String sql
    ) {
        DdlJob ddlJob = createViewWithInfoJob(schemaName, tableDefinition);
        ddlJob.setConnId(connId);
        try {
            doDdlJob(ddlJob);
        } catch (Exception e) {
            LogUtils.error(log, "[ddl-error] create table error,reason:" + e.getMessage() + ", tabDef" + tableDefinition, e);
            throw e;
        }
    }

    public void dropTable(SchemaInfo schemaInfo, Long tableId, String tableName, String connId) {
        DdlJob job = DdlJob.builder()
            .actionType(ActionType.ActionDropTable)
            .tableName(tableName)
            .tableId(tableId)
            .schemaId(schemaInfo.getSchemaId())
            .schemaName(schemaInfo.getName())
            .schemaState(schemaInfo.getSchemaState())
            .build();
        job.setConnId(connId);
        try {
            doDdlJob(job);
        } catch (Exception e) {
            LogUtils.error(log, "[ddl-error] drop table error,reason:" + e.getMessage() + ", tabDef" + tableName, e);
            throw e;
        }
    }

    public static void createSequence(String schema, String table, SequenceDefinition definition, String connId) {
        SchemaInfo schemaInfo = InfoSchemaService.root().getSchema(schema);
        if (schemaInfo == null) {
            throw new RuntimeException("schema not exists");
        }
        long schemaId = schemaInfo.getSchemaId();
        TableDefinitionWithId tableInfo = (TableDefinitionWithId) InfoSchemaService.root().getTable(schemaId, table);
        if (tableInfo == null) {
            throw new RuntimeException("table not exists");
        }
        DdlJob job = DdlJob.builder()
            .schemaId(schemaId)
            .tableId(tableInfo.getTableId().getEntityId())
            .schemaName(schema)
            .tableName(table)
            .actionType(ActionType.ActionCreateSequence)
            .schemaState(SchemaState.SCHEMA_NONE)
            .build();
        List<Object> args = new ArrayList<>();
        args.add(definition);
        job.setArgs(args);
        job.setConnId(connId);
        try {
            doDdlJob(job);
        } catch (Exception e) {
            LogUtils.error(log, "[ddl-error] create sequence error, reason:" + e.getMessage(), e);
            throw e;
        }
    }

    public static void dropSequence(String schema, String table, String sequenceName, String connId) {
        SchemaInfo schemaInfo = InfoSchemaService.root().getSchema(schema);
        if (schemaInfo == null) {
            throw new RuntimeException("schema not exists");
        }
        long schemaId = schemaInfo.getSchemaId();
        Table tableInfo = InfoSchemaService.root().getTableDef(schemaId, table);
        if (table == null) {
            throw new RuntimeException("table not exists");
        }
        DdlJob job = DdlJob.builder()
            .schemaId(schemaId)
            .tableId(tableInfo.getTableId().seq)
            .schemaName(schema)
            .tableName(table)
            .actionType(ActionType.ActionDropSequence)
            .build();
        List<Object> args = new ArrayList<>();
        args.add(sequenceName);
        job.setArgs(args);
        job.setConnId(connId);
        try {
            doDdlJob(job);
        } catch (Exception e) {
            LogUtils.error(log, "[ddl-error] drop sequence error, schema name:{}, sequence name:{}", schema, sequenceName, e);
            throw e;
        }
    }

    public void createSchema(String schemaName, long schemaId, String connId) {
        SchemaInfo schemaInfo = SchemaInfo.builder()
            .name(schemaName)
            .build();
        //InfoSchemaService infoSchemaService = InfoSchemaService.root();
        //assert infoSchemaService != null;
        //long schemaId = infoSchemaService.genSchemaId();
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
            LogUtils.error(log, "[ddl-error] createSchema error, reason:" + e.getMessage(), e);
            throw e;
        }
    }

    public void dropSchema(SchemaInfo schemaInfo, String connId) {
        DdlJob job = DdlJob.builder()
            .actionType(ActionType.ActionDropSchema)
            .schemaState(schemaInfo.getSchemaState())
            .schemaName(schemaInfo.getName())
            .schemaId(schemaInfo.getSchemaId()).build();
        job.setConnId(connId);
        try {
            doDdlJob(job);
        } catch (Exception e) {
            LogUtils.error(log, "[ddl-error] dropSchema error, schema:" + schemaInfo.getName(), e);
            throw e;
        }
    }

    public void truncateTable(SchemaInfo schemaInfo, Table table, String connId) {
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
            TsoService.getDefault().cacheTso(),
            CreateIdsRequest.builder()
                .idEpochType(IdEpochType.ID_NEXT_TABLE).count(1)
                .build()).getIds().get(0);
        List<Object> args = new ArrayList<>();
        args.add(tableEntityId);
        job.setArgs(args);
        try {
            doDdlJob(job);
        } catch (Exception e) {
            LogUtils.error(log, "[ddl-error] truncate table error, table:" + table.getName(), e);
            throw e;
        }
    }

    public void createIndex(String schemaName, String tableName, TableDefinition indexDef) {
        SchemaInfo schemaInfo = InfoSchemaService.root().getSchema(schemaName);
        if (schemaInfo == null) {
            throw new RuntimeException("schema not exists");
        }
        long schemaId = schemaInfo.getSchemaId();
        TableDefinitionWithId tableInfo = (TableDefinitionWithId) InfoSchemaService.root()
            .getTable(schemaId, tableName);
        if (tableInfo == null) {
            throw new RuntimeException("table not exists");
        }
        long tableId = tableInfo.getTableId().getEntityId();
        DdlJob job = DdlJob.builder()
            .schemaId(schemaId)
            .tableId(tableId)
            .schemaName(schemaName)
            .tableName(tableName)
            .actionType(ActionType.ActionAddIndex)
            .build();
        List<Object> args = new ArrayList<>();
        indexDef.setSchemaState(SchemaState.SCHEMA_NONE);
        args.add(indexDef);
        job.setArgs(args);
        try {
            doDdlJob(job);
        } catch (Exception e) {
            LogUtils.error(log, "[ddl-error] createIndex error, tableName:" + tableName, e);
            throw e;
        }
    }

    public void dropIndex(String schemaName, String tableName, String indexName) {
        SchemaInfo schemaInfo = InfoSchemaService.root().getSchema(schemaName);
        if (schemaInfo == null) {
            throw new RuntimeException("schema not exists");
        }
        long schemaId = schemaInfo.getSchemaId();
        Table table = InfoSchemaService.root().getTableDef(schemaId, tableName);
        if (table == null) {
            throw new RuntimeException("table not exists");
        }
        boolean notExists = table.getIndexes()
            .stream()
            .noneMatch(indexTable -> indexTable.getName().equalsIgnoreCase(indexName));
        if (notExists) {
            throw new RuntimeException("index not exists");
        }

        long tableId = table.tableId.seq;
        DdlJob job = DdlJob.builder()
            .schemaId(schemaId)
            .tableId(tableId)
            .schemaName(schemaName)
            .tableName(tableName)
            .actionType(ActionType.ActionDropIndex)
            .build();
        List<Object> args = new ArrayList<>();
        args.add(indexName);
        job.setArgs(args);
        doDdlJob(job);
    }

    public void addColumn(SchemaInfo schemaInfo, Table table, ColumnDefinition column, String connId) {
        DdlJob job = DdlJob.builder()
            .schemaId(schemaInfo.getSchemaId())
            .tableId(table.tableId.seq)
            .schemaName(schemaInfo.getName())
            .tableName(table.getName())
            .actionType(ActionType.ActionAddColumn)
            .schemaState(column.getSchemaState())
            .build();
        List<Object> args = new ArrayList<>();
        args.add(column);
        job.setArgs(args);
        doDdlJob(job);
    }

    public void dropColumn(
        SchemaInfo schemaInfo,
        Table table,
        String columnName,
        String markDel, String relatedIndex,
        String connId
    ) {
        List<Object> args = new ArrayList<>();
        args.add(columnName);
        args.add(markDel);
        args.add(relatedIndex);
        DdlJob job = DdlJob.builder()
            .schemaId(schemaInfo.getSchemaId())
            .tableId(table.tableId.seq)
            .schemaName(schemaInfo.getName())
            .tableName(table.getName())
            .schemaState(SchemaState.SCHEMA_PUBLIC)
            .actionType(ActionType.ActionDropColumn)
            .build();
        job.setArgs(args);
        try {
            doDdlJob(job);
        } catch (Exception e) {
            LogUtils.error(log, "[ddl-error] dropColumn error, tableName:{}, columnName:{}",
                table.getName(), columnName, e);
            throw e;
        }
    }

    public void modifyColumn(
        long schemaId, String schemaName, long tableId, List<ModifyingColInfo> modifyingColInfoList
    ) {
        List<Object> args = new ArrayList<>(modifyingColInfoList);
        DdlJob ddlJob = DdlJob.builder()
            .schemaName(schemaName)
            .schemaId(schemaId)
            .tableId(tableId)
            .actionType(ActionType.ActionModifyColumn)
            .args(args)
            .state(JobState.jobStateQueueing)
            .schemaState(SchemaState.SCHEMA_NONE)
            .build();
        doDdlJob(ddlJob);
    }

    public void changeColumn(
        long schemaId, String schemaName, long tableId, ModifyingColInfo modifyingColInfo
    ) {
        List<Object> args = new ArrayList<>();
        args.add(modifyingColInfo);
        DdlJob ddlJob = DdlJob.builder()
            .schemaName(schemaName)
            .schemaId(schemaId)
            .tableId(tableId)
            .actionType(ActionType.ActionModifyColumn)
            .args(args)
            .state(JobState.jobStateQueueing)
            .schemaState(SchemaState.SCHEMA_NONE)
            .build();
        doDdlJob(ddlJob);
    }

    public static DdlJob createTableWithInfoJob(String schemaName, TableDefinition tableDefinition) {
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        assert infoSchemaService != null;
        SchemaInfo schemaInfo = infoSchemaService.getSchema(schemaName);
        List<Object> args = new ArrayList<>();
        args.add(tableDefinition);

        CoordinatorService coordinatorService = Services.coordinatorService(Configuration.coordinatorSet());
        long tableEntityId = coordinatorService.createIds(
            TsoService.getDefault().cacheTso(),
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

    public static DdlJob createViewWithInfoJob(String schemaName, TableDefinition tableDefinition) {
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
            .actionType(ActionType.ActionCreateView)
            .state(JobState.jobStateQueueing)
            .args(args)
            .tableId(tableEntityId)
            .id(0)
            .build();
    }

    public static void doDdlJob(DdlJob job) {
        long timeout = ScopeVariables.getDdlWaitTimeout();
        if (job.mayNeedReorg()) {
            timeout = 0;
        }
        // put job to queue
        DdlJobEventSource.forcePut(asyncJobQueue, job);
        // get history job from history
        long start = System.currentTimeMillis();
        while (!Thread.interrupted()) {
            Pair<Boolean, DingoErr> res = historyJob(job.getId());
            if (res.getKey()) {
                return;
            } else if (res.getValue() != null) {
                LogUtils.error(log, "[ddl-error] doDdlJob error, reason: {}, job: {}", res.getValue(), job);
                throw DingoErrUtil.toMysqlError(res.getValue());
            }
            Utils.sleep(50);
            if (timeout == 0) {
                continue;
            }
            long sub = System.currentTimeMillis() - start;
            if (sub > timeout) {
                throw new RuntimeException("wait ddl timeout");
            }
        }
        try {
            InfoSchemaService.root().delHistoryDDLJob(job.getId());
        } catch (Exception e) {
            LogUtils.error(log, "[ddl-error] ddl handler del history job:" + e.getMessage(), e);
        }
    }

    public static Pair<Boolean, DingoErr> historyJob(long jobId) {
        DdlJob ddlJob = getHistoryJobById(jobId);
        if (ddlJob == null) {
            if (insertFailedJobIdList.containsKey(jobId)) {
                String error = insertFailedJobIdList.get(jobId);
                insertFailedJobIdList.remove(jobId);
                return Pair.of(false, DingoErrUtil.newInternalErr(error));
            } else {
                return Pair.of(false, null);
            }
        }
        if (ddlJob.getState() == JobState.jobStateSynced && ddlJob.getError() == null) {
            return Pair.of(true, null);
        }
        if (ddlJob.getError() != null) {
            if (ddlJob.getDingoErr() == null || ddlJob.getDingoErr().errorCode == 0) {
                String error = ddlJob.decodeError();
                return Pair.of(false, DingoErrUtil.newInternalErr(error));
            } else {
                return Pair.of(false, ddlJob.getDingoErr());
            }
        }
        return Pair.of(false, null);
    }

    public static DdlJob getHistoryJobById(long jobId) {
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        return infoSchemaService.getHistoryDDLJob(jobId);
    }

    public void recoverTable(RecoverInfo recoverInfo) {
        DdlJob ddlJob = DdlJob.builder()
            .schemaState(SchemaState.SCHEMA_NONE)
            .actionType(ActionType.ActionRecoverTable)
            .schemaId(recoverInfo.getSchemaId())
            .schemaName(recoverInfo.getOldSchemaName())
            .tableId(recoverInfo.getTableId())
            .state(JobState.jobStateQueueing)
            .build();
        List<Object> args = new ArrayList<>();
        args.add(recoverInfo);
        ddlJob.setArgs(args);
        try {
            doDdlJob(ddlJob);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            throw e;
        }
    }

    public void recoverSchema(RecoverInfo recoverInfo) {
        DdlJob ddlJob = DdlJob.builder()
            .schemaState(SchemaState.SCHEMA_NONE)
            .actionType(ActionType.ActionRecoverSchema)
            .schemaId(recoverInfo.getSchemaId())
            .schemaName(recoverInfo.getOldSchemaName())
            .state(JobState.jobStateQueueing)
            .build();
        List<Object> args = new ArrayList<>();
        args.add(recoverInfo);
        ddlJob.setArgs(args);
        try {
            doDdlJob(ddlJob);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            throw e;
        }
    }

}
