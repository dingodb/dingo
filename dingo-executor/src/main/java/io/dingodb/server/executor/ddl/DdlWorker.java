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
import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.ddl.JobState;
import io.dingodb.common.ddl.ReorgInfo;
import io.dingodb.common.ddl.SchemaDiff;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.session.Session;
import io.dingodb.common.table.IndexDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.util.Pair;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.service.entity.meta.ColumnDefinition;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.store.proxy.mapper.Mapper;
import io.dingodb.store.proxy.mapper.MapperImpl;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static io.dingodb.sdk.service.entity.common.SchemaState.SCHEMA_DELETE_ONLY;
import static io.dingodb.sdk.service.entity.common.SchemaState.SCHEMA_DELETE_REORG;
import static io.dingodb.sdk.service.entity.common.SchemaState.SCHEMA_NONE;
import static io.dingodb.sdk.service.entity.common.SchemaState.SCHEMA_PUBLIC;
import static io.dingodb.sdk.service.entity.common.SchemaState.SCHEMA_WRITE_ONLY;
import static io.dingodb.sdk.service.entity.common.SchemaState.SCHEMA_WRITE_REORG;

@Slf4j
public class DdlWorker {

    @Getter
    private final Session session;

    public DdlWorker(Session session) {
        this.session = session;
    }

    public void beginTxn() {
        this.session.setAutoCommit(false);
    }

    public void end() {
        this.session.setAutoCommit(true);
    }

    public Pair<Long, String> handleDDLJobTable(DdlContext dc, DdlJob job) {
        if (job.isDone() || job.isRollbackDone()) {
            if (job.isDone()) {
                job.setState(JobState.jobStateSynced);
            }
            String error = handleJobDone(job);
            return Pair.of(0L, error);
        }
        dc.rLock();
        // onJobRunBefore
        onJobRunBefore(job);
        dc.rUnlock();
        Pair<Long, String> res = runDdlJob(dc, job);
        if (res == null) {
            LogUtils.error(log, "run ddl job get res null");
            return Pair.of(0L, "run ddl job get res null");
        }
        long schemaVer = res.getKey();
        if (job.isCancelled()) {
            LogUtils.warn(log, "[ddl] job is cancelled, txn will rollback, jobId:{}", job.getId());
            // session reset
            session.rollback();
            String error = handleJobDone(job);
            //dc.getSv().unlockSchemaVersion(job);
            LogUtils.warn(log, "[ddl] job is cancelled, handleJobDone, jobId:{}", job.getId());
            return Pair.of(0L, error);
        }
        if (res.getValue() != null && !job.isRollingback() && !job.isRollbackDone()) {
            // session reset
            LogUtils.warn(log, "[ddl] job txn will rollback, jobId:{}", job.getId());
            session.rollback();
            LogUtils.warn(log, "[ddl] job txn rollback done, jobId:{}", job.getId());
            schemaVer = 0;
        }
        long start = System.currentTimeMillis();
        String error = registerMDLInfo(job, schemaVer);
        long end = System.currentTimeMillis();
        DingoMetrics.timer("registerMDLInfo").update((end - start), TimeUnit.MILLISECONDS);
        if (error != null) {
            session.rollback();
            //dc.getSv().unlockSchemaVersion(job);
            LogUtils.warn(log, "[ddl] registerMdlInfo failed, reason:{}, jobId:{}", error, job.getId());
            return Pair.of(0L, error);
        }
        error = updateDDLJob(job, res.getValue() != null);
        long sub = System.currentTimeMillis() - start;
        DingoMetrics.timer("updateDDLJob").update(sub, TimeUnit.MILLISECONDS);
        if (error != null) {
            // session rollback
            session.rollback();
            // unlockSchemaVersion
            //dc.getSv().unlockSchemaVersion(job);
            LogUtils.warn(log, "[ddl] update ddl job failed, reason:{}, jobId:{}", error, job.getId());
            return Pair.of(0L, error);
        }
        try {
            // session commit;
            session.commit();
        } catch (Exception e) {
            LogUtils.error(log, "[ddl] run and update ddl job commit error," + e.getMessage(), e);
        } finally {
            // unlockSchemaVersion
            //dc.getSv().unlockSchemaVersion(job);
        }

        registerSync(dc, job);
        if (res.getValue() != null) {
            LogUtils.info(log, "[ddl] run DDL job failed, sleeps a while then retries it, error:" + res.getValue());
            Utils.sleep(100);
        }
        return Pair.of(schemaVer, null);
    }

    public static void registerSync(DdlContext dc, DdlJob ddlJob) {
        dc.getWc().getLock().writeLock().lock();
        dc.getWc().getJobMap().put(ddlJob.getId(), ddlJob.getId());
        dc.getWc().getLock().writeLock().unlock();
    }

    public String registerMDLInfo(DdlJob job, long ver) {
        if (ver == 0) {
            return null;
        }
//        List<Object[]> rows;
//        try {
//            rows = this.session.executeQuery("select table_ids from mysql.dingo_ddl_job where job_id = " + job.getId());
//        } catch (Exception e) {
//            LogUtils.error(log, e.getMessage(), e);
//            return e.getMessage();
//        }
//        if (rows.isEmpty()) {
//            return "can't find ddl job " + job.getId();
//        }
//        String ids = (String) rows.get(0)[0];
        String ids = job.job2TableIDs();
        boolean duplicate = false;
//        try {
//            List<Object[]> res = session.executeQuery("select job_id from mysql.dingo_mdl_info where job_id=" + job.getId());
//            if (!res.isEmpty()) {
//                duplicate = true;
//            }
//        } catch (Exception e) {
//            LogUtils.error(log, e.getMessage(), e);
//        }
        String sql;
        if (duplicate) {
            DingoMetrics.counter("registerMDLInfoDuplicate").inc();
            sql = "update mysql.dingo_mdl_info set version=%d and table_ids=%s where job_id=%d";
            sql = String.format(sql, ver, Utils.quoteForSql(ids), job.getId());
        } else {
            DingoMetrics.counter("registerMDLInfoNone").inc();
            sql = "insert into mysql.dingo_mdl_info (job_id, version, table_ids) values (%d, %d, %s)";
            sql = String.format(sql, job.getId(), ver, Utils.quoteForSql(ids));
        }
        return session.executeUpdate(sql);
    }

    public String updateDDLJob(DdlJob job, boolean error) {
        boolean updateRawArgs = needUpdateRawArgs(job, error);

        return JobTableUtil.updateDDLJob2Table(session, job, updateRawArgs);
    }

    public static boolean needUpdateRawArgs(DdlJob job, boolean meetErr) {
        //if (meetErr && job.getRawArgs() != null && job.getArgs() == null) {
        //    return job.getMultiSchemaInfo() != null;
        //}
        return true;
    }

    public String handleJobDone(DdlJob job) {
        String error = finishDDLJob(job);
        if (error != null) {
            this.session.rollback();
            return error;
        }
        this.session.commit();
        // clean ddl reorg
        //ReorgUtil.cleanupDDLReorgHandles(job);
        return null;
    }

    public Pair<Long, String> runDdlJob(DdlContext dc, DdlJob job) {
        if (job.getRealStartTs() == 0) {
            job.setRealStartTs(System.currentTimeMillis());
        }
        if (job.isFinished()) {
            LogUtils.debug(log, "[ddl] finish DDL job, job:{}", job);
            return Pair.of(0L, null);
        }
        if (job.isCancelling()) {
            LogUtils.debug(log, "[ddl] cancel DDL job, job:{}", job);
            // convertJob2RollbackJob
            return DdlRollBack.convertJob2RollbackJob(this, dc, job);
        }
        if (!job.isRollingback() && !job.isCancelling()) {
            job.setState(JobState.jobStateRunning);
        }
        Pair<Long, String> res = null;
        switch (job.getActionType()) {
            case ActionCreateTable:
                res = onCreateTable(dc, job);
                break;
            case ActionCreateSchema:
                res = onCreateSchema(dc, job);
                break;
            case ActionDropSchema:
                res = onDropSchema(dc, job);
                break;
            case ActionDropTable:
                res = onDropTable(dc, job);
                break;
            case ActionTruncateTable:
                res = onTruncateTable(dc, job);
                break;
            case ActionAddIndex:
                res = onCreateIndex(dc, job);
                break;
            case ActionDropIndex:
                res = onDropIndex(dc, job);
                break;
            case ActionDropColumn:
                res = onDropColumn(dc, job);
                break;
            case ActionAddColumn:
                res = onAddColumn(dc, job);
                break;
            default:
                job.setState(JobState.jobStateCancelled);
                break;
        }
        assert res != null;
        String error = res.getValue();
        long version = res.getKey();
        if (error != null) {
            error = countForError(job, error);
        }
        LogUtils.info(log, "[ddl] runDdlJob done, jobId:{}", job.getId());
        return Pair.of(version, error);
    }

    private static String countForError(DdlJob job, String error) {
        job.setError(error);
        job.setErrorCount(job.getErrorCount() + 1);
        if (job.getState() == JobState.jobStateCancelled) {
            LogUtils.info(log, "[ddl] DDL job is cancelled normally");
        }
        LogUtils.error(log, "[ddl] run DDL job error," + error + ", jobId:{}", job.getId());
        if (job.getErrorCount() > 5 && job.getState() == JobState.jobStateRunning && job.isRollbackable()) {
            LogUtils.warn(log, "[ddl] DDL job error count exceed the limit, cancelling it now, jobId:{}", job.getId());
            job.setState(JobState.jobStateCancelling);
        }
        return error;
    }

    public static Pair<Long, String> onCreateSchema(DdlContext dc, DdlJob job) {
        long schemaId = job.getSchemaId();
        // get schemaInfo
        try {
            job.decodeArgs();
        } catch (Exception e) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, e.getMessage());
        }
        SchemaInfo schemaInfo = (SchemaInfo) job.getArgs().get(0);

        schemaInfo.setSchemaId(schemaId);
        schemaInfo.setSchemaState(SchemaState.SCHEMA_NONE);
        // checkSchemaNotExists
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        if (infoSchemaService.getSchema(schemaId) != null) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, "The database already exists");
        }
        SchemaInfo schemaInfoTmp = infoSchemaService.getSchema(schemaInfo.getName());
        if (schemaInfoTmp != null && schemaInfoTmp.getSchemaState() == SchemaState.SCHEMA_PUBLIC) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, "The database already exists");
        }

        if (schemaInfo.getSchemaState() == SchemaState.SCHEMA_NONE) {
            schemaInfo.setSchemaState(SchemaState.SCHEMA_PUBLIC);
            InfoSchemaService service = InfoSchemaService.root();
            service.createSchema(schemaId, schemaInfo);
            // finish job
            Pair<Long, String> res = updateSchemaVersion(dc, job);
            if (res.getValue() != null) {
                LogUtils.error(log, "onCreateSchema update schema version error:{}", res.getValue());
                return res;
            }
            long version = res.getKey();
            job.finishDBJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC, version, schemaInfo);
            return Pair.of(version, null);
        }
        return Pair.of(0L, "invalid db state");
    }

    public static Pair<Long, String> onDropSchema(DdlContext dc, DdlJob ddlJob) {
        Pair<SchemaInfo, String> resSchemaInfo = checkSchemaExistAndCancelNotExistJob(ddlJob);
        if (resSchemaInfo.getValue() != null) {
            return Pair.of(0L, resSchemaInfo.getValue());
        }
        SchemaInfo schemaInfo = resSchemaInfo.getKey();
        if (schemaInfo == null) {
            return Pair.of(0L, "ErrDatabaseDropExists");
        }
        Pair<Long, String> res = updateSchemaVersion(dc, ddlJob);
        if (res.getValue() != null) {
            return res;
        }
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        assert infoSchemaService != null;
        switch (schemaInfo.getSchemaState()) {
            case SCHEMA_PUBLIC:
                schemaInfo.setSchemaState(SchemaState.SCHEMA_WRITE_ONLY);
                infoSchemaService.updateSchema(schemaInfo);
                break;
            case SCHEMA_WRITE_ONLY:
                schemaInfo.setSchemaState(SchemaState.SCHEMA_DELETE_ONLY);
                infoSchemaService.updateSchema(schemaInfo);
                break;
            case SCHEMA_DELETE_ONLY:
                schemaInfo.setSchemaState(SchemaState.SCHEMA_NONE);
                infoSchemaService.updateSchema(schemaInfo);
                infoSchemaService.dropSchema(schemaInfo.getSchemaId());
                ddlJob.finishDBJob(JobState.jobStateDone, SchemaState.SCHEMA_NONE, res.getKey(), schemaInfo);
                break;
            default:
                break;
        }
        ddlJob.setSchemaState(schemaInfo.getSchemaState());
        return Pair.of(res.getKey(), null);
    }

    public static Pair<Long, String> onCreateTable(DdlContext dc, DdlJob ddlJob) {
        String err = ddlJob.decodeArgs();
        if (err != null) {
            ddlJob.setState(JobState.jobStateCancelled);
            return Pair.of(0L, err);
        }
        Pair<TableDefinition, String> res = TableUtil.createTable(ddlJob);
        if (res.getValue() != null) {
            return Pair.of(0L, res.getValue());
        }
        LogUtils.info(log, "[ddl] create table info and region done, jobId:{}", ddlJob.getId());

        Pair<Long, String> res1 = updateSchemaVersion(dc, ddlJob);

        if (res1.getValue() != null) {
            return res1;
        }
        ddlJob.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
        LogUtils.debug(log, "[ddl] onCreateTable done, jobId:{}", ddlJob.getId());
        return res1;
    }

    public static Pair<Long, String> onTruncateTable(DdlContext dc, DdlJob job) {
        MetaService rootMs = MetaService.root();
        MetaService ms = rootMs.getSubMetaService(job.getSchemaName());
        job.decodeArgs();
        long newTableId = 0;
        if (job.getArgs() != null) {
            newTableId = (long) job.getArgs().get(0);
        }
        Pair<Long, String> res;
        if (job.getSchemaState() == SchemaState.SCHEMA_PUBLIC) {
            job.setSchemaState(SchemaState.SCHEMA_GLOBAL_TXN_ONLY);
            return updateSchemaVersion(dc, job);
        }
        if ("Lock wait timeout exceeded".equalsIgnoreCase(job.getError())) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, job.getError());
        }
        try {
            ms.truncateTable(job.getTableName(), newTableId);
        } catch (Exception e) {
            LogUtils.error(log, "truncate table error", e);
        }
        //job.setTableId(tableId);
        res = updateSchemaVersion(dc, job);
        if (res.getValue() != null) {
            return res;
        }
        job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
        return res;
    }

    public static Pair<Long, String> onDropTable(DdlContext dc, DdlJob job) {
        Pair<TableDefinitionWithId, String> tableRes = checkTableExistAndCancelNonExistJob(job, job.getSchemaId());
        if (tableRes.getValue() != null && tableRes.getKey() == null) {
            return Pair.of(0L, tableRes.getValue());
        }
        TableDefinitionWithId tableInfo = tableRes.getKey();
        if (tableInfo == null) {
            return Pair.of(0L, "table not exists");
        }
        if ("Lock wait timeout exceeded".equalsIgnoreCase(job.getError())
            && tableInfo.getTableDefinition().getSchemaState() != SCHEMA_PUBLIC) {
            tableInfo.getTableDefinition().setSchemaState(SCHEMA_PUBLIC);
            ActionType originType = job.getActionType();
            job.setActionType(ActionType.ActionCreateTable);
            job.setState(JobState.jobStateCancelling);
            Pair<Long, String> res = TableUtil.updateVersionAndTableInfos(dc, job, tableInfo, true);
            job.setActionType(originType);
            DdlContext.INSTANCE.getSchemaSyncer().ownerUpdateExpVersion(res.getKey());
            return res;
        }
        SchemaState originalState = job.getSchemaState();
        Pair<Long, String> res;
        switch (tableInfo.getTableDefinition().getSchemaState()) {
            case SCHEMA_PUBLIC:
                tableInfo.getTableDefinition()
                    .setSchemaState(SCHEMA_WRITE_ONLY);
                res = TableUtil.updateVersionAndTableInfos(dc, job, tableInfo,
                    originalState.getCode() != tableInfo.getTableDefinition().getSchemaState().number());
                if (res.getValue() != null) {
                    return res;
                }
                break;
            case SCHEMA_WRITE_ONLY:
//                tableInfo.getTableDefinition().setSchemaState(SCHEMA_DELETE_ONLY);
//                res = TableUtil.updateVersionAndTableInfos(dc, job, tableInfo,
//                    originalState.getCode() != tableInfo.getTableDefinition().getSchemaState().number());
//                if (res.getValue() != null) {
//                    return res;
//                }
//                break;
//            case SCHEMA_DELETE_ONLY:
                tableInfo.getTableDefinition().setSchemaState(SCHEMA_NONE);
                long start = System.currentTimeMillis();
                res = TableUtil.updateVersionAndTableInfos(dc, job, tableInfo,
                    originalState.getCode() != tableInfo.getTableDefinition().getSchemaState().number());
                long sub = System.currentTimeMillis() - start;
                DingoMetrics.timer("updateVerAndTable").update(sub, TimeUnit.MILLISECONDS);
                if (res.getValue() != null) {
                    return res;
                }
                MetaService rootMs = MetaService.root();
                MetaService ms = rootMs.getSubMetaService(job.getSchemaName());
                try {
                    start = System.currentTimeMillis();
                    ms.dropTable(job.getSchemaId(), tableInfo.getTableDefinition().getName());
                    sub = System.currentTimeMillis() - start;
                    DingoMetrics.timer("metaDropTable").update(sub, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    LogUtils.error(log, "drop table error", e);
                }
                job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_NONE);
                break;
            default:
                return Pair.of(0L, "ErrInvalidDDLState");
        }
        job.setSchemaStateNumber(tableInfo.getTableDefinition().getSchemaState().number);
        return res;
    }

    public Pair<Long, String> onCreateIndex(DdlContext dc, DdlJob job) {
        //if (job.isRollingback()) {
            //return onDropIndex(dc, job);
        //}
        long schemaId = job.getSchemaId();
        Pair<TableDefinitionWithId, String> tableInfoRes = TableUtil.getTableInfoAndCancelFaultJob(job, schemaId);
        if (tableInfoRes.getValue() != null) {
            return Pair.of(0L, tableInfoRes.getValue());
        }
        String error = job.decodeArgs();
        if (error != null) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, error);
        }
        // get indexInfoWithId
        IndexDefinition indexInfo = (IndexDefinition) job.getArgs().get(0);
        //TableDefinitionWithId indexDef = null;
        // check index exists
        Table table = InfoSchemaService.root().getTableDef(job.getSchemaId(), job.getTableId());
        if (table == null) {
            return Pair.of(0L, "table not exists");
        }
        boolean exists = table.getIndexes().stream()
            .anyMatch(indexTable -> indexTable.getName().equalsIgnoreCase(indexInfo.getName())
            && indexTable.getSchemaState() == SchemaState.SCHEMA_PUBLIC);
        if (exists) {
            return Pair.of(0L, "index exists");
        }

        // check index column too many
        // check too many indexes
        ScopeVariables.testIndexBlock();
        SchemaState originState = indexInfo.getSchemaState();
        switch (indexInfo.getSchemaState()) {
            case SCHEMA_NONE:
                // update index def
                try {
                    indexInfo.setSchemaState(SchemaState.SCHEMA_DELETE_ONLY);
                    MetaService.root().createIndex(table.tableId, table.getName(), indexInfo);
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage(), e);
                    return Pair.of(0L, "create index error");
                }
                IndexUtil.pickBackFillType(job);
                job.setSchemaState(SchemaState.SCHEMA_DELETE_ONLY);
                return updateSchemaVersion(dc, job);
            case SCHEMA_DELETE_ONLY:
                indexInfo.setSchemaState(SchemaState.SCHEMA_WRITE_ONLY);
                TableDefinitionWithId indexWithId = IndexUtil.getIndexWithId(table, indexInfo.getName());
                indexWithId.getTableDefinition().setSchemaState(SCHEMA_WRITE_ONLY);
                job.setSchemaState(SchemaState.SCHEMA_WRITE_ONLY);
                return TableUtil.updateVersionAndIndexInfos(dc, job, indexWithId, indexInfo.getSchemaState() != originState);
            case SCHEMA_WRITE_ONLY:
                indexInfo.setSchemaState(SchemaState.SCHEMA_WRITE_REORG);
                indexWithId = IndexUtil.getIndexWithId(table, indexInfo.getName());
                indexWithId.getTableDefinition().setSchemaState(SCHEMA_WRITE_REORG);
                job.setSnapshotVer(0);
                job.setSchemaState(SchemaState.SCHEMA_WRITE_REORG);
                return TableUtil.updateVersionAndIndexInfos(dc, job, indexWithId, indexInfo.getSchemaState() != originState);
            case SCHEMA_WRITE_REORG:
                Pair<Boolean, Long> reorgRes;
                try {
                    IndexUtil index = IndexUtil.INSTANCE;
                    indexWithId = IndexUtil.getIndexWithId(table, indexInfo.getName());
                    reorgRes = index.doReorgWorkForCreateIndex(dc, job, this, table.tableId, indexWithId);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    return Pair.of(0L, e.getMessage());
                }
                assert reorgRes != null;
                if (!reorgRes.getKey()) {
                    return Pair.of(reorgRes.getValue(), "[ddl] doReorg failed");
                }
                indexInfo.setSchemaState(SchemaState.SCHEMA_PUBLIC);
                job.setSchemaState(SchemaState.SCHEMA_PUBLIC);
                job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
                indexWithId.getTableDefinition().setSchemaState(SCHEMA_PUBLIC);
                // update version and index info
                return TableUtil.updateVersionAndIndexInfos(dc, job, indexWithId, originState != indexInfo.getSchemaState());
            default:
                error = "ErrInvalidDDLState";
                break;
        }
        return Pair.of(0L, error);
    }

    public static Pair<Long, String> onDropIndex(DdlContext dc, DdlJob job) {
        String error = job.decodeArgs();
        if (error != null) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, error);
        }
        Table table = InfoSchemaService.root().getTableDef(job.getSchemaId(), job.getTableId());
        String indexName = job.getArgs().get(0).toString();
        boolean notExists = table.getIndexes().stream()
            .noneMatch(indexTable -> indexTable.getName().equalsIgnoreCase(indexName));
        if (notExists) {
            return Pair.of(0L, "index not exists");
        }
        TableDefinitionWithId indexWithId = IndexUtil.getIndexWithId(table, indexName);
        io.dingodb.sdk.service.entity.common.SchemaState originState = indexWithId.getTableDefinition().getSchemaState();
        switch (indexWithId.getTableDefinition().getSchemaState()) {
            case SCHEMA_PUBLIC:
                indexWithId.getTableDefinition().setSchemaState(SCHEMA_WRITE_ONLY);
                job.setSchemaState(SchemaState.SCHEMA_WRITE_ONLY);
                return TableUtil.updateVersionAndIndexInfos(dc, job, indexWithId,
                    originState != indexWithId.getTableDefinition().getSchemaState()
                );
            case SCHEMA_WRITE_ONLY:
                indexWithId.getTableDefinition().setSchemaState(SCHEMA_DELETE_ONLY);
                job.setSchemaState(SchemaState.SCHEMA_DELETE_ONLY);
                return TableUtil.updateVersionAndIndexInfos(dc, job, indexWithId,
                    originState != indexWithId.getTableDefinition().getSchemaState()
                );
            case SCHEMA_DELETE_ONLY:
                indexWithId.getTableDefinition().setSchemaState(SCHEMA_DELETE_REORG);
                job.setSchemaState(SchemaState.SCHEMA_DELETE_REORG);
                return TableUtil.updateVersionAndIndexInfos(dc, job, indexWithId,
                    originState != indexWithId.getTableDefinition().getSchemaState()
                );
            case SCHEMA_DELETE_REORG:
                indexWithId.getTableDefinition().setSchemaState(SCHEMA_NONE);
                job.setSchemaState(SchemaState.SCHEMA_NONE);
                if (job.isRollingback()) {
                    job.finishTableJob(JobState.jobStateRollbackDone, SchemaState.SCHEMA_NONE);
                } else {
                    job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_NONE);
                }
                Pair<Long, String> res = TableUtil.updateVersionAndIndexInfos(dc, job, indexWithId,
                    originState != indexWithId.getTableDefinition().getSchemaState()
                );
                try {
                    MetaService.root().dropIndex(table.getTableId(), Mapper.MAPPER.idFrom(indexWithId.getTableId()));
                } catch (Exception e) {
                    LogUtils.error(log, "drop index error", e);
                }
                return res;
            default:
                error = "ErrInvalidDDLState";
                break;
        }

        return Pair.of(0L, error);
    }

    public Pair<Long, String> onDropColumn(DdlContext dc, DdlJob job) {
        String error = job.decodeArgs();
        if (error != null) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, error);
        }
        String columnName = job.getArgs().get(0).toString();
        String markDel = job.getArgs().get(1).toString();
        //String related = job.getArgs().get(2).toString();
        InfoSchemaService.root().getTable(job.getSchemaId(), job.getTableId());
        Pair<TableDefinitionWithId, String> tableInfoRes
            = TableUtil.getTableInfoAndCancelFaultJob(job, job.getSchemaId());
        if (tableInfoRes.getValue() != null) {
            return Pair.of(0L, tableInfoRes.getValue());
        }
        TableDefinitionWithId tableWithId = tableInfoRes.getKey();
        ColumnDefinition columnDef = tableWithId.getTableDefinition().getColumns()
            .stream().filter(columnDefinition -> columnDefinition.getName().equalsIgnoreCase(columnName))
            .findFirst().orElse(null);
        if (columnDef == null) {
            return Pair.of(0L, "not found drop column");
        }
        List<TableDefinitionWithId> markDelIndices = null;
        if (StringUtils.isNotEmpty(markDel)) {
            Table table = InfoSchemaService.root().getTableDef(job.getSchemaId(), job.getTableId());
            if (StringUtils.isNotEmpty(markDel)) {
                markDelIndices = new ArrayList<>();
                String[] markDelList = markDel.split(",");
                for (String markDelIndex : markDelList) {
                    markDelIndices.add(IndexUtil.getIndexWithId(table, markDelIndex));
                }
            }
        }
        switch (job.getSchemaState()) {
            case SCHEMA_PUBLIC:
                //columnDef.setSchemaState(SCHEMA_WRITE_ONLY);
                String originTableName = tableWithId.getTableDefinition().getName();
                DdlColumn.setIndicesState(markDelIndices, SCHEMA_WRITE_ONLY);
                tableWithId.getTableDefinition().setSchemaState(SCHEMA_DELETE_ONLY);
                tableWithId.getTableDefinition()
                    .getColumns()
                    .removeIf(columnDefinition -> columnDefinition.getName().equalsIgnoreCase(columnName));
                tableWithId.getTableDefinition().setName(DdlUtil.ddlTmpTableName);
                MetaService.root().createReplicaTable(job.getSchemaId(), tableWithId, originTableName);
                IndexUtil.pickBackFillType(job);
                job.setSchemaState(SchemaState.SCHEMA_WRITE_ONLY);
                return updateSchemaVersion(dc, job);
            case SCHEMA_WRITE_ONLY:
                TableDefinitionWithId withId = (TableDefinitionWithId) InfoSchemaService.root()
                    .getReplicaTable(job.getSchemaId(), job.getTableId(), 0);
                withId.getTableDefinition().setSchemaState(SCHEMA_WRITE_ONLY);
                TableUtil.updateReplicaTable(job.getSchemaId(), job.getTableId(), withId);
                job.setSchemaState(SchemaState.SCHEMA_DELETE_ONLY);
                DdlColumn.setIndicesState(markDelIndices, SCHEMA_DELETE_ONLY);
                return updateSchemaVersion(dc, job);
            case SCHEMA_DELETE_ONLY:
                withId = (TableDefinitionWithId) InfoSchemaService.root()
                    .getReplicaTable(job.getSchemaId(), job.getTableId(), 0);
                withId.getTableDefinition().setSchemaState(SCHEMA_WRITE_REORG);
                TableUtil.updateReplicaTable(job.getSchemaId(), job.getTableId(), withId);
                job.setSchemaState(SchemaState.SCHEMA_DELETE_REORG);
                DdlColumn.setIndicesState(markDelIndices, SCHEMA_DELETE_REORG);
                return updateSchemaVersion(dc, job);
            case SCHEMA_DELETE_REORG:
                withId = (TableDefinitionWithId) InfoSchemaService.root()
                    .getReplicaTable(job.getSchemaId(), job.getTableId(), 0);
                DingoCommonId replicaTableId = withId.getTableId();
                // reorg
                CommonId tableId = MapperImpl.MAPPER.idFrom(tableWithId.getTableId());
                try {
                    IndexUtil.INSTANCE.doReorgWorkForCreateIndex(dc, job, this, tableId, withId);
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage(), e);
                    return Pair.of(0L, "reorg failed");
                }
                withId.getTableDefinition().setSchemaState(SCHEMA_PUBLIC);
                TableUtil.updateReplicaTable(job.getSchemaId(), job.getTableId(), withId);
                withId.setTableId(tableWithId.getTableId());
                withId.getTableDefinition().setName(tableWithId.getTableDefinition().getName());
                // replace replicaTable to table
                TableUtil.updateVersionAndTableInfos(dc, job, withId, true);
                try {
                    // to remove replica table
                    InfoSchemaService.root().dropIndex(tableId.seq, replicaTableId.getEntityId());
                    // remove old region
                    MetaService.root().deleteRegionByTableId(tableId);
                } catch (Exception e) {
                    LogUtils.error(log, "drop replicaTable error", e);
                }

                job.setSchemaState(SchemaState.SCHEMA_NONE);
                DdlColumn.setIndicesState(markDelIndices, SCHEMA_NONE);
                if (markDelIndices != null) {
                    try {
                        markDelIndices.forEach(index -> {
                            MetaService.root().deleteRegionByTableId(Mapper.MAPPER.idFrom(index.getTableId()));
                            InfoSchemaService.root().dropIndex(tableId.seq, index.getTableId().getEntityId());
                        });
                    } catch (Exception e) {
                        LogUtils.error(log, "drop mark del indices error", e);
                    }
                }
                if (job.isRollingback()) {
                    job.finishTableJob(JobState.jobStateRollbackDone, SchemaState.SCHEMA_PUBLIC);
                } else {
                    job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
                }
                return updateSchemaVersion(dc, job);
            default:
                error = "ErrInvalidDDLJob";
        }

        return Pair.of(0L, error);
    }

    public Pair<Long, String> onAddColumn(DdlContext dc, DdlJob job) {
        String error = job.decodeArgs();
        if (error != null) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, error);
        }
        io.dingodb.common.table.ColumnDefinition columnDefinition
            = (io.dingodb.common.table.ColumnDefinition) job.getArgs().get(0);
        if (!columnDefinition.isNullable() && columnDefinition.getDefaultValue() == null) {
            columnDefinition.setDefaultValue(DdlUtil.getColDefaultValIfNull(columnDefinition.getType()));
        }
        if (columnDefinition.getDefaultValue() != null && columnDefinition.getType() instanceof StringType) {
            String defaultVal = columnDefinition.getDefaultValue();
            if (defaultVal.startsWith("'") && defaultVal.endsWith("'")) {
                columnDefinition.setDefaultValue(defaultVal.substring(1, defaultVal.length() - 1));
            }
        }

        Pair<TableDefinitionWithId, String> tableRes = checkTableExistAndCancelNonExistJob(job, job.getSchemaId());
        if (tableRes.getValue() != null && tableRes.getKey() == null) {
            return Pair.of(0L, tableRes.getValue());
        }
        boolean exists = tableRes.getKey().getTableDefinition().getColumns().stream()
            .anyMatch(columnDefinition1 -> columnDefinition1.getName().equalsIgnoreCase(columnDefinition.getName()));
        if (exists) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, "Duplicate column name '" + columnDefinition.getName() + "'");
        }

        switch (columnDefinition.getSchemaState()) {
            case SCHEMA_NONE:
                TableDefinitionWithId definitionWithId = tableRes.getKey();
                String originTableName = definitionWithId.getTableDefinition().getName();
                definitionWithId.getTableDefinition().setSchemaState(SCHEMA_DELETE_ONLY);
                columnDefinition.setSchemaState(SchemaState.SCHEMA_DELETE_ONLY);
                List<ColumnDefinition> columnDefinitions = definitionWithId.getTableDefinition().getColumns();
                boolean withoutPriTable = columnDefinitions.stream()
                    .anyMatch(columnDefinition1 -> columnDefinition1.getState() == 2
                    && columnDefinition1.getName().equalsIgnoreCase("_ROWID")
                    && columnDefinition1.getIndexOfKey() >= 0);
                if (withoutPriTable) {
                    int colSize = columnDefinitions.size();
                    definitionWithId.getTableDefinition().getColumns()
                        .add(colSize - 1, MapperImpl.MAPPER.columnTo(columnDefinition));
                } else {
                    definitionWithId.getTableDefinition()
                        .getColumns().add(MapperImpl.MAPPER.columnTo(columnDefinition));
                }
                definitionWithId.getTableDefinition().setName("replicaTable");
                MetaService.root().createReplicaTable(job.getSchemaId(), definitionWithId, originTableName);
                job.setSchemaState(SchemaState.SCHEMA_DELETE_ONLY);
                return updateSchemaVersion(dc, job);
            case SCHEMA_DELETE_ONLY:
                TableDefinitionWithId withId = (TableDefinitionWithId) InfoSchemaService.root()
                        .getReplicaTable(job.getSchemaId(), job.getTableId(), 0);
                withId.getTableDefinition().setSchemaState(SCHEMA_WRITE_ONLY);
                columnDefinition.setSchemaState(SchemaState.SCHEMA_WRITE_ONLY);
                withId.getTableDefinition()
                    .getColumns()
                    .forEach(colDef -> {
                        if (colDef.getName().equalsIgnoreCase(columnDefinition.getName())) {
                            colDef.setSchemaState(SCHEMA_WRITE_ONLY);
                        }
                    });
                TableUtil.updateReplicaTable(job.getSchemaId(), job.getTableId(), withId);
                job.setSchemaState(SchemaState.SCHEMA_WRITE_ONLY);
                return updateSchemaVersion(dc, job);
            case SCHEMA_WRITE_ONLY:
                withId = (TableDefinitionWithId) InfoSchemaService.root()
                    .getReplicaTable(job.getSchemaId(), job.getTableId(), 0);
                withId.getTableDefinition().setSchemaState(SCHEMA_WRITE_REORG);
                withId.getTableDefinition()
                    .getColumns()
                    .forEach(colDef -> {
                        if (colDef.getName().equalsIgnoreCase(columnDefinition.getName())) {
                            colDef.setSchemaState(SCHEMA_WRITE_REORG);
                        }
                    });
                TableUtil.updateReplicaTable(job.getSchemaId(), job.getTableId(), withId);
                columnDefinition.setSchemaState(SchemaState.SCHEMA_WRITE_REORG);
                job.setSchemaState(SchemaState.SCHEMA_WRITE_REORG);
                return updateSchemaVersion(dc, job);
            case SCHEMA_WRITE_REORG:
                withId = (TableDefinitionWithId) InfoSchemaService.root()
                    .getReplicaTable(job.getSchemaId(), job.getTableId(), 0);
                DingoCommonId replicaTableId = withId.getTableId();
                // reorg
                CommonId tableId = MapperImpl.MAPPER.idFrom(tableRes.getKey().getTableId());
                try {
                    DdlColumn.doReorgWorkForAddCol(dc, job, tableId, withId, this);
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage(), e);
                    job.setState(JobState.jobStateCancelled);
                    job.setError("reorg failed");
                    return Pair.of(0L, "reorg failed");
                }
                withId.getTableDefinition()
                    .getColumns()
                    .forEach(colDef -> {
                        if (colDef.getName().equalsIgnoreCase(columnDefinition.getName())) {
                            colDef.setSchemaState(SCHEMA_PUBLIC);
                        }
                    });
                withId.getTableDefinition().setSchemaState(SCHEMA_PUBLIC);
                TableUtil.updateReplicaTable(job.getSchemaId(), job.getTableId(), withId);
                columnDefinition.setSchemaState(SchemaState.SCHEMA_PUBLIC);
                withId.setTableId(tableRes.getKey().getTableId());
                withId.getTableDefinition().setName(tableRes.getKey().getTableDefinition().getName());
                // replace replicaTable to table
                TableUtil.updateVersionAndTableInfos(dc, job, withId, true);
                try {
                    // to remove replica table
                    InfoSchemaService.root().dropIndex(tableId.seq, replicaTableId.getEntityId());
                    // remove old region
                    MetaService.root().deleteRegionByTableId(tableId);
                } catch (Exception e) {
                    LogUtils.error(log, "drop replicaTable error", e);
                }
                if (job.isRollingback()) {
                    job.finishTableJob(JobState.jobStateRollbackDone, SchemaState.SCHEMA_PUBLIC);
                } else {
                    job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
                }
                return updateSchemaVersion(dc, job);
            default:
                error = "ErrInvalidDDLJob";
        }
        return Pair.of(0L, error);
    }

    public static Pair<SchemaInfo, String> checkSchemaExistAndCancelNotExistJob(DdlJob job) {
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        assert infoSchemaService != null;
        SchemaInfo schemaInfo = (SchemaInfo) infoSchemaService.getSchema(job.getSchemaId());
        if (schemaInfo == null) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(null, "ErrDatabaseDropExists");
        }
        return Pair.of(schemaInfo, null);
    }

    public static Pair<TableDefinitionWithId, String> checkTableExistAndCancelNonExistJob(DdlJob job, long schemaId) {
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        try {
            Object tableObj = infoSchemaService.getTable(schemaId, job.getTableId());
            if (tableObj != null) {
                TableDefinitionWithId tableDefinitionWithId = (TableDefinitionWithId) tableObj;
                return Pair.of(tableDefinitionWithId, null);
            } else {
                return Pair.of(null, "ErrNotExists");
            }
        } catch (Exception e) {
            return Pair.of(null, e.getMessage());
        }
    }

    public static void onJobRunBefore(DdlJob job) {
        LogUtils.info(log, "on job run before,job:{}", job);
        if (job.getActionType() != ActionType.ActionModifyColumn) {
            return;
        }
        int schemaState = job.getSchemaState().getCode();
        if (schemaState == 1 || schemaState == 2 || schemaState == 3) {
            Utils.sleep(500);
        }
    }

    public static Pair<Long, String> updateSchemaVersion(DdlContext dc, DdlJob ddlJob) {
        long schemaVersion;
        try {
            schemaVersion = dc.getSv().setSchemaVersion(ddlJob);
            LogUtils.debug(log, "update version value:" + schemaVersion + ", jobId:" + ddlJob.getId());
        } catch (Exception e) {
            LogUtils.error(log, "updateSchemaVersion: setSchemaVer failed, reason:{}", e.getMessage());
            return Pair.of(0L, e.getMessage());
        } finally {
            dc.getSv().unlockSchemaVersion(ddlJob);
        }
        try {
            SchemaDiff schemaDiff = SchemaDiff
                .builder()
                .schemaId(ddlJob.getSchemaId())
                .version(schemaVersion)
                .type(ddlJob.getActionType())
                .build();
            switch (ddlJob.getActionType()) {
                case ActionTruncateTable:
                    String err = ddlJob.decodeArgs();
                    if (err != null) {
                        return Pair.of(0L, err);
                    }
                    long newTableId = 0;
                    if (ddlJob.getArgs() != null) {
                        newTableId = (long) ddlJob.getArgs().get(0);
                    }
                    schemaDiff.setTableId(newTableId);
                    schemaDiff.setOldTableId(ddlJob.getTableId());
                    break;
                default:
                    schemaDiff.setTableId(ddlJob.getTableId());
                    break;
            }
            InfoSchemaService infoSchemaService = InfoSchemaService.root();
            //LogUtils.info(log, "[ddl] put schemaDiff:{}", schemaDiff);

            infoSchemaService.setSchemaDiff(schemaDiff);
            DingoMetrics.counter("setSchemaDiff").inc();
            return Pair.of(schemaVersion, null);
        } catch (Exception e) {
            LogUtils.error(log, "[ddl-error] put schemaDiff error, jobId:" + ddlJob.getId() + ", version:" + schemaVersion, e);
            return Pair.of(0L, e.getMessage());
        } finally {
            LogUtils.info(log, "[ddl] updateSchemaVersion done, jobId:{}, version:{}", ddlJob.getId(), schemaVersion);
        }
    }

    public String finishDDLJob(DdlJob job) {
        String error = JobTableUtil.deleteDDLJob(session, job);
        if (error != null) {
            return error;
        }
        boolean updateRawArgs = job.getActionType() != ActionType.ActionAddPrimaryKey || job.isCancelled();
        return addHistoryDDLJob(session, job, updateRawArgs);
    }

    public static String addHistoryDDLJob(Session session, DdlJob job, boolean updateRawArgs) {
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        infoSchemaService.addHistoryDDLJob(job, false);
        //return JobTableUtil.addHistoryDDLJob2Table(session, job, updateRawArgs);
        return null;
    }

    public static String waitSchemaSyncedForMDL(DdlContext dc, DdlJob ddlJob, long latestSchemaVersion) {
        try {
            long start = System.currentTimeMillis();
            String error = dc.getSchemaSyncer()
                .ownerCheckAllVersions(ddlJob.getId(), latestSchemaVersion, ddlJob.mayNeedReorg());
            if (error != null) {
                LogUtils.error(log, error);
                return error;
            }
            long end = System.currentTimeMillis();
            LogUtils.info(log, "[ddl] wait latest schema version changed, ver:{}, " +
                "take time:{}, job:{}", latestSchemaVersion, (end - start), ddlJob.getId());
            return null;
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            return e.getMessage();
        }
    }

    public String runReorgJob(
        DdlContext dc,
        ReorgInfo reorgInfo,
        Function<Void, String> function
    ) {
        DdlJob job = reorgInfo.getDdlJob();
        ReorgCtx rc = dc.getReorgCtx1(job.getId());
        if (rc == null) {
            if (job.isCancelling()) {
                return "ErrCancelledDDLJob";
            }
            rc = newReorgCtx(reorgInfo);
            CompletableFuture<String> done = rc.getDone();
            Executors.execute("reorg", () -> {
                String error = function.apply(null);
                done.complete(error);
            });
        }

        try {
            String error = rc.getDone().get(3600, TimeUnit.SECONDS);
            if (rc.isReorgCanceled() || "ErrCancelledDDLJob".equalsIgnoreCase(error)) {
                dc.removeReorgCtx(job.getId());
                return "ErrCancelledDDLJob";
            }
            long rowCount = rc.getRowCount();
            if (error != null) {
                LogUtils.warn(log, "[ddl] run reorg job done, rows:{}, error:{}", rowCount, error);
            } else {
                LogUtils.debug(log, "[ddl] run reorg job done, rows:{}", rowCount);
            }
            job.setRowCount(rowCount);
            dc.removeReorgCtx(job.getId());
            if (error != null) {
                return error;
            }
        } catch (TimeoutException e1) {
            long rowCount = rc.getRowCount();
            job.setRowCount(rowCount);
            LogUtils.error(log, "run reorg job wait timeout, addCount:{}", rowCount);
            return "ErrWaitReorgTimeout";
        } catch (ExecutionException e) {
            long rowCount = rc.getRowCount();
            job.setRowCount(rowCount);
            LogUtils.error(log, "run reorg job execution error, addCount:{}", rowCount);
            return "ErrReorgExecution";
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public ReorgCtx newReorgCtx(ReorgInfo reorgInfo) {
        ReorgCtx reorgCtx = ReorgCtx.builder()
            .done(new CompletableFuture<>())
            .rowCount(reorgInfo.getDdlJob().getRowCount())
            .build();
        reorgCtx.setRowCount(reorgInfo.getDdlJob().getRowCount());
        DdlContext.INSTANCE.getReorgCtx().putReorg(reorgInfo.getDdlJob().getId(), reorgCtx);
        return reorgCtx;
    }

    public static void checkDropColumnForStatePublic(ColumnDefinition columnDefinition) {
        if (!columnDefinition.isNullable() && columnDefinition.getDefaultVal() == null) {
            DdlColumn.generateOriginDefaultValue(columnDefinition);
        }
    }
}
