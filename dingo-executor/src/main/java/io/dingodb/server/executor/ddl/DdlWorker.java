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
import io.dingodb.common.ddl.ModifyingColInfo;
import io.dingodb.common.ddl.RecoverInfo;
import io.dingodb.common.ddl.ReorgInfo;
import io.dingodb.common.ddl.SchemaDiff;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.mysql.DingoErr;
import io.dingodb.common.mysql.DingoErrUtil;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.sequence.SequenceDefinition;
import io.dingodb.common.session.Session;
import io.dingodb.common.table.IndexDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.util.Pair;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.SequenceService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.service.entity.meta.ColumnDefinition;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.store.proxy.mapper.Mapper;
import io.dingodb.store.proxy.mapper.MapperImpl;
import io.dingodb.store.proxy.service.AutoIncrementService;
import io.dingodb.tso.TsoService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.dingodb.common.mysql.error.ErrorCode.ErrCancelledDDLJob;
import static io.dingodb.common.mysql.error.ErrorCode.ErrCantDropFieldOrKey;
import static io.dingodb.common.mysql.error.ErrorCode.ErrDBCreateExists;
import static io.dingodb.common.mysql.error.ErrorCode.ErrDBDropExists;
import static io.dingodb.common.mysql.error.ErrorCode.ErrDupFieldName;
import static io.dingodb.common.mysql.error.ErrorCode.ErrInvalidDDLState;
import static io.dingodb.common.mysql.error.ErrorCode.ErrKeyDoesNotExist;
import static io.dingodb.common.mysql.error.ErrorCode.ErrNoSuchTable;
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
            LogUtils.warn(log, "[ddl] registerMdlInfo failed, reason:{}, jobId:{}", error, job.getId());
            return Pair.of(0L, error);
        }
        error = updateDDLJob(job, res.getValue() != null);
        long sub = System.currentTimeMillis() - start;
        DingoMetrics.timer("updateDDLJob").update(sub, TimeUnit.MILLISECONDS);
        if (error != null) {
            // session rollback
            session.rollback();
            LogUtils.warn(log, "[ddl] update ddl job failed, reason:{}, jobId:{}", error, job.getId());
            return Pair.of(0L, error);
        }
        try {
            // session commit;
            session.commit();
        } catch (Exception e) {
            LogUtils.error(log, "[ddl] run and update ddl job commit error," + e.getMessage(), e);
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
        String ids = job.job2TableIDs();
        DingoMetrics.counter("registerMDLInfoNone").inc();
        String sql = "insert into mysql.dingo_mdl_info (job_id, version, table_ids) values (%d, %d, %s)";
        sql = String.format(sql, job.getId(), ver, Utils.quoteForSql(ids));
        return session.executeUpdate(sql);
    }

    public String updateDDLJob(DdlJob job, boolean error) {
        return JobTableUtil.updateDDLJob2Table(session, job, true);
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
            job.setRealStartTs(TsoService.getDefault().tso());
        }
        if (job.isFinished()) {
            LogUtils.debug(log, "[ddl] finish DDL job, job:{}", job);
            return Pair.of(0L, null);
        }
        if (job.isCancelling()) {
            LogUtils.debug(log, "[ddl] cancel DDL job, job:{}", job);
            // convertJob2RollbackJob
            return DdlRollBack.convertJob2RollbackJob(job);
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
            case ActionCreateView:
                res = onCreateView(dc, job);
                break;
            case ActionDropView:
                res = onDropView(dc, job);
                break;
            case ActionRecoverTable:
                res = onRecoverTable(dc, job);
                break;
            case ActionRecoverSchema:
                res = onRecoverSchema(dc, job);
                break;
            case ActionCreateSequence:
                res = onCreateSequence(dc, job);
                break;
            case ActionDropSequence:
                res = onDropSequence(dc, job);
                break;
            case ActionModifyColumn:
                res = onModifyColumn(dc, job);
                break;
            case ActionRebaseAuto:
                res = onRebaseAuto(dc, job);
                break;
            case ActionResetAutoInc:
                res = onResetAutoInc(dc, job);
                break;
            case ActionRenameTable:
                res = onRenameTable(dc, job);
                break;
            case ActionRenameIndex:
                res = onRenameIndex(dc, job);
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
        LogUtils.info(log, "[ddl] runDdlJob done, jobId:{}, version:{}", job.getId(), version);
        return Pair.of(version, error);
    }

    private static String countForError(DdlJob job, String error) {
        job.encodeError(error);
        job.setErrorCount(job.getErrorCount() + 1);
        if (job.getState() == JobState.jobStateCancelled) {
            LogUtils.info(log, "[ddl] DDL job is cancelled normally");
        }
        LogUtils.error(log, "[ddl] run DDL job error," + error + ", jobId:{}", job.getId());
        if (job.getErrorCount() > 5 && job.getState() == JobState.jobStateRunning && job.isRollbackable()) {
            LogUtils.warn(log, "[ddl] DDL job error count exceed the limit, cancelling it now, jobId:{}",
                job.getId());
            job.setState(JobState.jobStateCancelling);
        } else if (job.getErrorCount() > 10) {
            LogUtils.error(log, "[ddl] DDL job error count exceed max limit,jobId:{}", job.getId());
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
            job.setDingoErr(DingoErrUtil.newInternalErr(ErrDBCreateExists, job.getSchemaName()));
            return Pair.of(0L, "The database already exists");
        }
        SchemaInfo schemaInfoTmp = infoSchemaService.getSchema(schemaInfo.getName());
        if (schemaInfoTmp != null && schemaInfoTmp.getSchemaState() == SchemaState.SCHEMA_PUBLIC) {
            job.setState(JobState.jobStateCancelled);
            job.setDingoErr(DingoErrUtil.newInternalErr(ErrDBCreateExists, job.getSchemaName()));
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
            ddlJob.setDingoErr(DingoErrUtil.newInternalErr(ErrDBDropExists, ddlJob.getSchemaName()));
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

    public static Pair<Long, String> onCreateSequence(DdlContext dc, DdlJob job) {
        String err = job.decodeArgs();
        if (err != null) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, err);
        }
        Table tableDef = InfoSchemaService.root().getTableDef(job.getSchemaId(), job.getTableId());
        if (tableDef == null) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, "table not exists");
        }
        SequenceDefinition sequenceDefinition = (SequenceDefinition) job.getArgs().get(0);
        SequenceService sequenceService = SequenceService.getDefault();
        sequenceService.createSequence(sequenceDefinition);
        job.setSchemaState(SchemaState.SCHEMA_PUBLIC);

        Pair<Long, String> res = updateSchemaVersion(dc, job);
        job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
        return res;
    }

    public static Pair<Long, String> onDropSequence(DdlContext dc, DdlJob job) {
        String err = job.decodeArgs();
        if (err != null) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, err);
        }
        Table tableDef = InfoSchemaService.root().getTableDef(job.getSchemaId(), job.getTableId());
        if (tableDef == null) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, "table not exists");
        }
        String sequence = (String) job.getArgs().get(0);
        SequenceService sequenceService = SequenceService.getDefault();
        sequenceService.dropSequence(sequence);
        job.setSchemaState(SchemaState.SCHEMA_PUBLIC);

        Pair<Long, String> res = updateSchemaVersion(dc, job);
        job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
        return res;
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
        if (job.getSchemaState() == SchemaState.SCHEMA_PUBLIC) {
            Pair<Long, String> res = updateSchemaVersion(dc, job);
            job.setSchemaState(SchemaState.SCHEMA_GLOBAL_TXN_ONLY);
            return res;
        }
        if (job.getError() != null) {
            String error = job.decodeError();
            if ("Lock wait timeout exceeded".equalsIgnoreCase(error)) {
                job.setState(JobState.jobStateCancelled);
                return Pair.of(0L, error);
            }
        }
        try {
            ms.truncateTable(job.getTableName(), newTableId, job.getId());
        } catch (Exception e) {
            LogUtils.error(log, "truncate table error", e);
        }
        //job.setTableId(tableId);
        Pair<Long, String> res = updateSchemaVersion(dc, job);
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
        if (job.getError() != null) {
            if ("Lock wait timeout exceeded".equalsIgnoreCase(job.decodeError())
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
                tableInfo.getTableDefinition().setSchemaState(SCHEMA_NONE);
                long start = System.currentTimeMillis();
                res = TableUtil.updateVersionAndTableInfos(dc, job, tableInfo,
                    originalState.getCode() != tableInfo.getTableDefinition().getSchemaState().number());
                long sub = System.currentTimeMillis() - start;
                DingoMetrics.timer("updateVerAndTable").update(sub, TimeUnit.MILLISECONDS);
                if (res.getValue() != null) {
                    return res;
                }
                try {
                    start = System.currentTimeMillis();
                    MetaService.root().dropTable(
                        job.getSchemaId(), tableInfo.getTableDefinition().getName(), job.getId()
                    );
                    sub = System.currentTimeMillis() - start;
                    DingoMetrics.timer("metaDropTable").update(sub, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    LogUtils.error(log, "drop table error", e);
                    return Pair.of(0L, "dropTableError:" + e.getMessage());
                }
                job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_NONE);
                break;
            default:
                return Pair.of(0L, "ErrInvalidDDLState:" + tableInfo.getTableDefinition().getSchemaState());
        }
        job.setSchemaStateNumber(tableInfo.getTableDefinition().getSchemaState().number);
        return res;
    }

    public Pair<Long, String> onCreateIndex(DdlContext dc, DdlJob job) {
        if (job.isRollingback()) {
            return onDropIndex(dc, job);
        }
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
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, "table not exists");
        }
        boolean exists = table.getIndexes().stream()
            .anyMatch(indexTable -> indexTable.getName().equalsIgnoreCase(indexInfo.getName())
                && indexTable.getSchemaState() == SchemaState.SCHEMA_PUBLIC);
        if (exists) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, "index exists");
        }

        // check index column too many
        // check too many indexes
        ScopeVariables.testIndexBlock();
        SchemaState originState = indexInfo.getSchemaState();
        TableDefinitionWithId indexWithId;
        switch (indexInfo.getSchemaState()) {
            case SCHEMA_NONE:
                // update index def
                try {
                    indexInfo.setSchemaState(SchemaState.SCHEMA_DELETE_ONLY);
                    MetaService.root().createIndex(table.tableId, table.getName(), indexInfo);
                } catch (Exception e) {
                    job.setState(JobState.jobStateCancelled);
                    LogUtils.error(log, e.getMessage(), e);
                    return Pair.of(0L, e.getMessage());
                }
                IndexUtil.pickBackFillType(job);
                job.setSchemaState(SchemaState.SCHEMA_DELETE_ONLY);
                return updateSchemaVersion(dc, job);
            case SCHEMA_DELETE_ONLY:
                indexInfo.setSchemaState(SchemaState.SCHEMA_WRITE_ONLY);
                indexWithId = IndexUtil.getIndexWithId(table, indexInfo.getName());
                indexWithId.getTableDefinition().setSchemaState(SCHEMA_WRITE_ONLY);
                job.setSchemaState(SchemaState.SCHEMA_WRITE_ONLY);
                return TableUtil.updateVersionAndIndexInfos(dc, job, indexWithId,
                    indexInfo.getSchemaState() != originState);
            case SCHEMA_WRITE_ONLY:
                indexInfo.setSchemaState(SchemaState.SCHEMA_WRITE_REORG);
                indexWithId = IndexUtil.getIndexWithId(table, indexInfo.getName());
                indexWithId.getTableDefinition().setSchemaState(SCHEMA_WRITE_REORG);
                job.setSnapshotVer(0);
                job.setSchemaState(SchemaState.SCHEMA_WRITE_REORG);
                return TableUtil.updateVersionAndIndexInfos(dc, job, indexWithId,
                    indexInfo.getSchemaState() != originState);
            case SCHEMA_WRITE_REORG:
                Pair<Boolean, Long> reorgRes;
                try {
                    IndexUtil index = IndexUtil.INSTANCE;
                    indexWithId = IndexUtil.getIndexWithId(table, indexInfo.getName());
                    reorgRes = index.doReorgWorkForCreateIndex(dc, job, this, table.tableId, indexWithId);
                } catch (Exception e) {
                    //job.setState(JobState.jobStateCancelled);
                    LogUtils.error(log, e.getMessage(), e);
                    return Pair.of(0L, e.getMessage());
                }
                assert reorgRes != null;
                if (!reorgRes.getKey()) {
                    job.setState(JobState.jobStateCancelled);
                    return Pair.of(reorgRes.getValue(), "[ddl] doReorg failed");
                }
                indexInfo.setSchemaState(SchemaState.SCHEMA_PUBLIC);
                job.setSchemaState(SchemaState.SCHEMA_PUBLIC);
                job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
                indexWithId.getTableDefinition().setSchemaState(SCHEMA_PUBLIC);
                // update version and index info
                return TableUtil.updateVersionAndIndexInfos(dc, job, indexWithId,
                    originState != indexInfo.getSchemaState());
            default:
                job.setDingoErr(DingoErrUtil.newInternalErr(ErrInvalidDDLState, "index",
                    indexInfo.getSchemaState().toString()));
                error = job.getDingoErr().errorMsg;
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
        String indexName;
        if (job.getArgs().get(0) instanceof IndexDefinition) {
            indexName = ((IndexDefinition) job.getArgs().get(0)).getName();
        } else {
            indexName = job.getArgs().get(0).toString();
        }
        boolean notExists = table.getIndexes().stream()
            .noneMatch(indexTable -> indexTable.getName().equalsIgnoreCase(indexName));
        if (notExists) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, "index not exists");
        }
        TableDefinitionWithId indexWithId = IndexUtil.getIndexWithId(table, indexName);
        io.dingodb.sdk.service.entity.common.SchemaState originState
            = indexWithId.getTableDefinition().getSchemaState();
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
                try {
                    MetaService.root().dropIndex(
                        table.getTableId(), Mapper.MAPPER.idFrom(indexWithId.getTableId()),
                        job.getId(), job.getRealStartTs()
                    );
                } catch (Exception e) {
                    LogUtils.error(log, "drop index error", e);
                }
                return updateSchemaVersion(dc, job);
            default:
                job.setDingoErr(DingoErrUtil.newInternalErr(
                    ErrInvalidDDLState, "index", indexWithId.getTableDefinition().getSchemaState().toString()));
                error = job.getDingoErr().errorMsg;
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
        //String related = job.getArgs().get(2).toString();
        InfoSchemaService.root().getTable(job.getSchemaId(), job.getTableId());
        Pair<TableDefinitionWithId, String> tableInfoRes
            = TableUtil.getTableInfoAndCancelFaultJob(job, job.getSchemaId());
        if (tableInfoRes.getValue() != null) {
            return Pair.of(0L, tableInfoRes.getValue());
        }
        TableDefinitionWithId tableWithId = tableInfoRes.getKey();
        String columnName = job.getArgs().get(0).toString();
        ColumnDefinition columnDef = tableWithId.getTableDefinition().getColumns()
            .stream().filter(columnDefinition -> columnDefinition.getName().equalsIgnoreCase(columnName))
            .findFirst().orElse(null);
        if (columnDef == null) {
            job.setState(JobState.jobStateCancelled);
            job.setDingoErr(DingoErrUtil.newInternalErr(ErrCantDropFieldOrKey, columnName));
            return Pair.of(0L, job.getDingoErr().errorMsg);
        }
        List<TableDefinitionWithId> markDelIndices = null;
        String markDel = job.getArgs().get(1).toString();
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
        TableDefinitionWithId withId;
        switch (job.getSchemaState()) {
            case SCHEMA_PUBLIC:
                //columnDef.setSchemaState(SCHEMA_WRITE_ONLY);
                DdlColumn.setIndicesState(markDelIndices, SCHEMA_WRITE_ONLY);
                tableWithId.getTableDefinition().setSchemaState(SCHEMA_DELETE_ONLY);
                tableWithId.getTableDefinition()
                    .getColumns()
                    .removeIf(columnDefinition -> columnDefinition.getName().equalsIgnoreCase(columnName));
                String originTableName = tableWithId.getTableDefinition().getName();
                tableWithId.getTableDefinition().setName(DdlUtil.ddlTmpTableName);
                MetaService.root().createReplicaTable(job.getSchemaId(), tableWithId, originTableName);
                IndexUtil.pickBackFillType(job);
                job.setSchemaState(SchemaState.SCHEMA_WRITE_ONLY);
                return updateSchemaVersion(dc, job);
            case SCHEMA_WRITE_ONLY:
                withId = (TableDefinitionWithId) InfoSchemaService.root()
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
                    DdlColumn.doReorgWorkForDropCol(dc, job, tableId, withId, this);
                } catch (Exception e) {
                    job.setState(JobState.jobStateCancelled);
                    MetaService.root().dropRegionByTable(
                        Mapper.MAPPER.idFrom(replicaTableId), job.getId(), job.getRealStartTs()
                    );
                    InfoSchemaService.root().dropIndex(tableId.seq, replicaTableId.getEntityId());
                    LogUtils.error(log, e.getMessage(), e);
                    updateSchemaVersion(dc, job);
                    return Pair.of(0L, e.getMessage());
                }
                withId.getTableDefinition().setSchemaState(SCHEMA_PUBLIC);
                TableUtil.updateReplicaTable(job.getSchemaId(), job.getTableId(), withId);
                withId.setTableId(tableWithId.getTableId());
                withId.getTableDefinition().setName(tableWithId.getTableDefinition().getName());
                // replace replicaTable to table
                try {
                    // to remove replica table
                    InfoSchemaService.root().dropIndex(tableId.seq, replicaTableId.getEntityId());
                    // remove old region
                    MetaService.root().dropRegionByTable(tableId, job.getId(), job.getRealStartTs());
                } catch (Exception e) {
                    LogUtils.error(log, "drop replicaTable error", e);
                }

                job.setSchemaState(SchemaState.SCHEMA_NONE);
                DdlColumn.setIndicesState(markDelIndices, SCHEMA_NONE);
                if (markDelIndices != null) {
                    try {
                        markDelIndices.forEach(index -> {
                            MetaService.root().dropRegionByTable(
                                Mapper.MAPPER.idFrom(index.getTableId()), job.getId(), job.getRealStartTs()
                            );
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
                return TableUtil.updateVersionAndTableInfos(dc, job, withId, true);
            default:
                job.setDingoErr(DingoErrUtil.newInternalErr(
                    ErrInvalidDDLState, "column", job.getSchemaState().toString()));
                error = job.getDingoErr().errorMsg;
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
            job.setDingoErr(DingoErrUtil.newInternalErr(ErrDupFieldName, columnDefinition.getName()));
            return Pair.of(0L, job.getDingoErr().errorMsg);
        }
        TableDefinitionWithId withId;
        switch (columnDefinition.getSchemaState()) {
            case SCHEMA_NONE:
                TableDefinitionWithId definitionWithId = tableRes.getKey();
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
                String originTableName = definitionWithId.getTableDefinition().getName();
                definitionWithId.getTableDefinition().setName("replicaTable");
                MetaService.root().createReplicaTable(job.getSchemaId(), definitionWithId, originTableName);
                job.setSchemaState(SchemaState.SCHEMA_DELETE_ONLY);
                return updateSchemaVersion(dc, job);
            case SCHEMA_DELETE_ONLY:
                withId = (TableDefinitionWithId) InfoSchemaService.root()
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
                    MetaService.root().dropRegionByTable(
                        Mapper.MAPPER.idFrom(replicaTableId), job.getId(), job.getRealStartTs()
                    );
                    InfoSchemaService.root().dropIndex(tableId.seq, replicaTableId.getEntityId());
                    job.setState(JobState.jobStateCancelled);
                    updateSchemaVersion(dc, job);
                    return Pair.of(0L, e.getMessage());
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
                try {
                    // to remove replica table
                    InfoSchemaService.root().dropIndex(tableId.seq, replicaTableId.getEntityId());
                    // remove old region
                    MetaService.root().dropRegionByTable(tableId, job.getId(), job.getRealStartTs());
                } catch (Exception e) {
                    LogUtils.error(log, "drop replicaTable error", e);
                }
                if (job.isRollingback()) {
                    job.finishTableJob(JobState.jobStateRollbackDone, SchemaState.SCHEMA_PUBLIC);
                } else {
                    job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
                }
                return TableUtil.updateVersionAndTableInfos(dc, job, withId, true);
            default:
                job.setDingoErr(DingoErrUtil.newInternalErr(
                    ErrInvalidDDLState, "column", columnDefinition.getSchemaState().toString()));
                error = job.getDingoErr().errorMsg;
        }
        return Pair.of(0L, error);
    }

    public Pair<Long, String> onModifyColumn(DdlContext dc, DdlJob job) {
        // get schemaInfo tableInfo columnInfo modifyColumnInfo
        String error = job.decodeArgs();
        if (error != null) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, error);
        }
        Pair<TableDefinitionWithId, String> tableRes = checkTableExistAndCancelNonExistJob(job, job.getSchemaId());
        if (tableRes.getValue() != null) {
            return Pair.of(0L, tableRes.getValue());
        }
        ModifyingColInfo modifyingColInfo = (ModifyingColInfo) job.getArgs().get(0);
        // check tableInfo schemaState is public
        // check oldCol schemaState is null

        // if job rolling back -> rollbackModifyColumnJob/rollbackModifyColumnJobWithData

        io.dingodb.common.table.ColumnDefinition oldColDef = modifyingColInfo.getChangingCol();
        io.dingodb.common.table.ColumnDefinition newColDef = modifyingColInfo.getNewCol();
        // If we want to rename the column name, we need to check whether it already exists.
        if (oldColDef != null && !oldColDef.getName().equals(newColDef.getName())) {
            TableDefinitionWithId tableWithId = tableRes.getKey();
            boolean dupColName = tableWithId.getTableDefinition().getColumns()
                .stream().anyMatch(columnDefinition -> columnDefinition.getName().equals(newColDef.getName()));
            if (dupColName) {
                job.setState(JobState.jobStateCancelled);
                job.setDingoErr(DingoErrUtil.newInternalErr(ErrDupFieldName, newColDef.getName()));
                return Pair.of(0L, job.getDingoErr().errorMsg);
            }
        }
        // if do not need change column data -> doModifyColumn
        if (oldColDef == null || !needModifyColWIthData(oldColDef, newColDef)) {
            return doModifyColumn(modifyingColInfo, job, dc);
        }
        // doModifyColumnWithData
        return doModifyColumnTypeWithData(modifyingColInfo, job, dc);
    }

    public static Pair<SchemaInfo, String> checkSchemaExistAndCancelNotExistJob(DdlJob job) {
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        assert infoSchemaService != null;
        SchemaInfo schemaInfo = (SchemaInfo) infoSchemaService.getSchema(job.getSchemaId());
        if (schemaInfo == null) {
            job.setState(JobState.jobStateCancelled);
            job.setDingoErr(DingoErrUtil.newInternalErr(ErrDBDropExists, job.getSchemaName()));
            return Pair.of(null, job.getDingoErr().errorMsg);
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
                job.setState(JobState.jobStateCancelled);
                job.setDingoErr(DingoErrUtil.newInternalErr(ErrNoSuchTable, job.getTableName()));
                return Pair.of(null, job.getDingoErr().errorMsg);
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
                .schemaState(ddlJob.getSchemaState())
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
            LogUtils.error(log, "[ddl-error] put schemaDiff error, jobId:" + ddlJob.getId()
                + ", version:" + schemaVersion, e);
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
        if (job.getActionType() == ActionType.ActionRecoverTable) {
            finishRecoverTable(job);
        }
        boolean updateRawArgs = job.getActionType() != ActionType.ActionAddPrimaryKey || job.isCancelled();
        return addHistoryDDLJob(session, job, updateRawArgs);
    }

    public static String addHistoryDDLJob(Session session, DdlJob job, boolean updateRawArgs) {
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        infoSchemaService.addHistoryDDLJob(job, false);
        if (ScopeVariables.getJob2Table()) {
            return JobTableUtil.addHistoryDDLJob2Table(session, job, updateRawArgs);
        } else {
            return null;
        }
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
            LogUtils.info(log, "[ddl] wait latest schema version changed, ver:{}, "
                + "take time:{}, job:{}", latestSchemaVersion, (end - start), ddlJob.getId());
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
                job.setDingoErr(DingoErrUtil.newInternalErr(ErrCancelledDDLJob));
                return job.getDingoErr().errorMsg;
            }
            rc = newReorgCtx(reorgInfo);
            CompletableFuture<Object> done = rc.getDone();
            Executors.execute("reorg", () -> {
                String error;
                try {
                    error = function.apply(null);
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage(), e);
                    if (e instanceof NullPointerException) {
                        error = "NullPointerException";
                    } else {
                        error = e.getMessage();
                    }
                }
                done.complete(error);
            });
        }

        try {
            Object error = rc.getDone().get();
            long rowCount = rc.getRowCount();
            job.setRowCount(rowCount);
            dc.removeReorgCtx(job.getId());
            if (error != null) {
                LogUtils.error(log, "[ddl] run reorg job done, rows:{}, error:{}", rowCount, error);
                return error.toString();
            } else {
                LogUtils.debug(log, "[ddl] run reorg job done, rows:{}", rowCount);
            }
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

    public DingoErr runReorgJobWithStdErr(
        DdlContext dc,
        ReorgInfo reorgInfo,
        Function<Void, String> function
    ) {
        DdlJob job = reorgInfo.getDdlJob();
        ReorgCtx rc = dc.getReorgCtx1(job.getId());
        if (rc == null) {
            if (job.isCancelling()) {
                job.setDingoErr(DingoErrUtil.newInternalErr(ErrCancelledDDLJob));
                return job.getDingoErr();
            }
            rc = newReorgCtx(reorgInfo);
            CompletableFuture<Object> done = rc.getDone();
            Executors.execute("reorg", () -> {
                DingoErr dingoErr = null;
                try {
                    function.apply(null);
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage(), e);
                    dingoErr = DingoErrUtil.fromException(e);
                }
                done.complete(dingoErr);
            });
        }

        try {
            Object error = rc.getDone().get();
            long rowCount = rc.getRowCount();
            job.setRowCount(rowCount);
            dc.removeReorgCtx(job.getId());
            if (error != null) {
                LogUtils.error(log, "[ddl] run reorg job done, rows:{}, error:{}", rowCount, error);
                DingoErr err = (DingoErr) error;
                job.setDingoErr(err);
                return err;
            } else {
                LogUtils.debug(log, "[ddl] run reorg job done, rows:{}", rowCount);
            }
        } catch (ExecutionException e) {
            long rowCount = rc.getRowCount();
            job.setRowCount(rowCount);
            LogUtils.error(log, "run reorg job execution error, addCount:{}", rowCount);
            return DingoErrUtil.fromException(e);
        } catch (InterruptedException e) {
            return DingoErrUtil.fromException(e);
        }
        return DingoErrUtil.normal();
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

    public static Pair<Long, String> onCreateView(DdlContext dc, DdlJob ddlJob) {
        String err = ddlJob.decodeArgs();
        if (err != null) {
            ddlJob.setState(JobState.jobStateCancelled);
            return Pair.of(0L, err);
        }
        Pair<TableDefinition, String> res = TableUtil.createView(ddlJob);
        if (res.getValue() != null) {
            return Pair.of(0L, res.getValue());
        }
        LogUtils.info(log, "[ddl] create view info done, jobId:{}", ddlJob.getId());

        Pair<Long, String> res1 = updateSchemaVersion(dc, ddlJob);

        if (res1.getValue() != null) {
            return res1;
        }
        ddlJob.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
        LogUtils.debug(log, "[ddl] onCreateView done, jobId:{}", ddlJob.getId());
        return res1;
    }

    public static Pair<Long, String> onDropView(DdlContext dc, DdlJob job) {
        Pair<TableDefinitionWithId, String> tableRes = checkTableExistAndCancelNonExistJob(job, job.getSchemaId());
        if (tableRes.getValue() != null && tableRes.getKey() == null) {
            return Pair.of(0L, tableRes.getValue());
        }
        TableDefinitionWithId tableInfo = tableRes.getKey();
        if (tableInfo == null) {
            job.setDingoErr(DingoErrUtil.newInternalErr(ErrNoSuchTable, job.getTableName()));
            return Pair.of(0L, job.getDingoErr().errorMsg);
        }
        if (job.getError() != null) {
            if ("Lock wait timeout exceeded".equalsIgnoreCase(job.decodeError())
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
                //tableInfo.getTableDefinition().setSchemaState(SCHEMA_DELETE_ONLY);
                //res = TableUtil.updateVersionAndTableInfos(dc, job, tableInfo,
                //    originalState.getCode() != tableInfo.getTableDefinition().getSchemaState().number());
                //if (res.getValue() != null) {
                //    return res;
                //}
                //break;
                //case SCHEMA_DELETE_ONLY:
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
                    ms.dropTable(job.getSchemaId(), tableInfo.getTableDefinition().getName(), job.getId());
                    sub = System.currentTimeMillis() - start;
                    DingoMetrics.timer("metaDropView").update(sub, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    LogUtils.error(log, "drop view error", e);
                }
                job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_NONE);
                break;
            default:
                job.setDingoErr(DingoErrUtil.newInternalErr(
                    ErrInvalidDDLState, "view", job.getSchemaState().toString()));
                return Pair.of(0L, job.getDingoErr().errorMsg);
        }
        job.setSchemaStateNumber(tableInfo.getTableDefinition().getSchemaState().number);
        return res;
    }

    public Pair<Long, String> onRecoverTable(DdlContext dc, DdlJob job) {
        // checkEnableGc
        String enableGcStr = InfoSchemaService.root().getGlobalVariables().get("enable_safe_point_update");
        boolean enableGc = Boolean.parseBoolean(enableGcStr);

        // get tableInfo
        job.decodeArgs();

        RecoverInfo recoverInfo = (RecoverInfo) job.getArgs().get(0);
        long tableId = job.getTableId();
        // schemaState none: save flg
        // write-only: disableGc -> checkSafePoint -> recoverTable -> updateVersionAndTableInfo
        // -> public -> finishTable
        String error;
        switch (job.getSchemaState()) {
            case SCHEMA_NONE:
                recoverInfo.setEnableGc(enableGc);
                job.setSchemaState(SchemaState.SCHEMA_WRITE_ONLY);
                return updateSchemaVersion(dc, job);
            case SCHEMA_WRITE_ONLY:
                if (enableGc) {
                    // disableGc
                    InfoSchemaService.root().putGlobalVariable("enable_safe_point_update", "0");
                }
                // recover table
                InfoSchemaService infoSchemaService
                    = new io.dingodb.store.service.InfoSchemaService(recoverInfo.getSnapshotTs());
                TableDefinitionWithId tableDefinitionWithId = (TableDefinitionWithId) infoSchemaService
                    .getTable(recoverInfo.getSchemaId(), tableId);
                List<Object> indexList = infoSchemaService.listIndex(job.getSchemaId(), job.getTableId());
                tableDefinitionWithId.getTableDefinition().setSchemaState(SCHEMA_PUBLIC);
                if (recoverInfo.getNewTableName() != null) {
                    tableDefinitionWithId.getTableDefinition().setName(recoverInfo.getNewTableName());
                }
                TableUtil.recoverTable(job, recoverInfo, tableDefinitionWithId, indexList);
                job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
                return updateSchemaVersion(dc, job);
            default:
                job.setDingoErr(DingoErrUtil.newInternalErr(
                    ErrInvalidDDLState, "table", job.getSchemaState().toString()));
                error = job.getDingoErr().errorMsg;
        }
        return Pair.of(0L, error);
    }

    public Pair<Long, String> doModifyColumn(
        ModifyingColInfo modifyColumnInfo, DdlJob job, DdlContext dc
    ) {
        Pair<TableDefinitionWithId, String> tableRes = checkTableExistAndCancelNonExistJob(job, job.getSchemaId());
        if (tableRes.getValue() != null && tableRes.getKey() == null) {
            return Pair.of(0L, tableRes.getValue());
        }

        TableDefinitionWithId definitionWithId = tableRes.getKey();

        // modify col def
        // find modify col index
        // new col
        // replace
        int colIndex = -1;
        for (int i = 0; i < definitionWithId.getTableDefinition().getColumns().size(); i++) {
            ColumnDefinition col = definitionWithId.getTableDefinition().getColumns().get(i);
            if (col.getName().equals(modifyColumnInfo.getOldColName())) {
                colIndex = i;
                break;
            }
        }
        ColumnDefinition columnDefinition = MapperImpl.MAPPER.columnTo(modifyColumnInfo.getNewCol());
        columnDefinition.setSchemaState(SCHEMA_PUBLIC);
        definitionWithId.getTableDefinition().getColumns().set(colIndex, columnDefinition);

        // modify index column
        Table table = InfoSchemaService.root().getTableDef(job.getSchemaId(), job.getTableId());
        table.getIndexes().forEach(indexTable -> {
            AtomicInteger idx = new AtomicInteger(-1);
            for (int i = 0; i < indexTable.getColumns().size(); i++) {
                Column col = indexTable.getColumns().get(i);
                if (col.getName().equals(modifyColumnInfo.getOldColName())) {
                    idx.set(i);
                    break;
                }
            }
            if (idx.get() > -1) {
                TableDefinitionWithId indexWithId = IndexUtil.getIndexWithId(table, indexTable.getName());
                indexWithId.getTableDefinition().getColumns().set(idx.get(), columnDefinition);
                TableUtil.updateVersionAndIndexInfos(dc, job, indexWithId, false);
            }
        });
        job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
        return TableUtil.updateVersionAndTableInfos(dc, job, definitionWithId, true);
    }

    public Pair<Long, String> doModifyColumnTypeWithData(
        ModifyingColInfo modifyColumnInfo, DdlJob job, DdlContext dc
    ) {
        Pair<TableDefinitionWithId, String> tableRes = checkTableExistAndCancelNonExistJob(job, job.getSchemaId());
        if (tableRes.getValue() != null && tableRes.getKey() == null) {
            return Pair.of(0L, tableRes.getValue());
        }
        TableDefinitionWithId withId;
        List<Object> indexWithIdList;
        String error;
        switch (modifyColumnInfo.getNewCol().getSchemaState()) {
            case SCHEMA_NONE:
                TableDefinitionWithId definitionWithId = tableRes.getKey();
                definitionWithId.getTableDefinition().setSchemaState(SCHEMA_DELETE_ONLY);
                modifyColumnInfo.getNewCol().setSchemaState(SchemaState.SCHEMA_DELETE_ONLY);
                // modify col def
                // find modify col index
                // new col
                // replace
                int colIndex = -1;
                for (int i = 0; i < definitionWithId.getTableDefinition().getColumns().size(); i++) {
                    ColumnDefinition col = definitionWithId.getTableDefinition().getColumns().get(i);
                    if (col.getName().equals(modifyColumnInfo.getOldColName())) {
                        colIndex = i;
                        break;
                    }
                }
                ColumnDefinition columnDefinition = MapperImpl.MAPPER.columnTo(modifyColumnInfo.getNewCol());
                columnDefinition.setSchemaState(SCHEMA_PUBLIC);
                definitionWithId.getTableDefinition().getColumns().set(colIndex, columnDefinition);

                String originTableName = definitionWithId.getTableDefinition().getName();
                definitionWithId.getTableDefinition().setName(DdlUtil.ddlTmpTableName);
                long originPriTabId = definitionWithId.getTableId().getEntityId();
                MetaService.root().createReplicaTable(job.getSchemaId(), definitionWithId, originTableName);

                // doHandle index start
                Table table = InfoSchemaService.root().getTableDef(job.getSchemaId(), job.getTableId());
                table.getIndexes().forEach(indexTable -> {
                    if (indexTable.getName().startsWith(DdlUtil.ddlTmpTableName)) {
                        return;
                    }
                    AtomicInteger idx = new AtomicInteger(-1);
                    for (int i = 0; i < indexTable.getColumns().size(); i++) {
                        Column col = indexTable.getColumns().get(i);
                        if (col.getName().equals(modifyColumnInfo.getOldColName())) {
                            idx.set(i);
                            break;
                        }
                    }
                    if (idx.get() > -1) {
                        TableDefinitionWithId indexWithId = IndexUtil.getIndexWithId(table, indexTable.getName());
                        String originIndexName = indexWithId.getTableDefinition().getName();
                        indexWithId.getTableDefinition().setSchemaState(SCHEMA_DELETE_ONLY);
                        indexWithId.getTableDefinition().getColumns().set(idx.get(), columnDefinition);
                        indexWithId.getTableDefinition().setName(DdlUtil.ddlTmpIndexName + "_" + originIndexName);
                        MetaService.root().createIndexReplicaTable(
                            job.getSchemaId(), originPriTabId,
                            indexWithId, originIndexName);
                    }
                });
                // doHandle index end

                job.setSchemaState(SchemaState.SCHEMA_DELETE_ONLY);
                return updateSchemaVersion(dc, job);
            case SCHEMA_DELETE_ONLY:
                withId = (TableDefinitionWithId) InfoSchemaService.root()
                    .getReplicaTable(job.getSchemaId(), job.getTableId(), 0);
                withId.getTableDefinition().setSchemaState(SCHEMA_WRITE_ONLY);
                TableUtil.updateReplicaTable(job.getSchemaId(), job.getTableId(), withId);

                modifyColumnInfo.getNewCol().setSchemaState(SchemaState.SCHEMA_WRITE_ONLY);
                job.setSchemaState(SchemaState.SCHEMA_WRITE_ONLY);

                // handle index
                indexWithIdList = InfoSchemaService.root()
                    .getReplicaIndex(job.getSchemaId(), job.getTableId());
                indexWithIdList.forEach(indexObj -> {
                    TableDefinitionWithId indexWithId = (TableDefinitionWithId) indexObj;
                    indexWithId.getTableDefinition().setSchemaState(SCHEMA_WRITE_ONLY);
                    TableUtil.updateReplicaTable(job.getSchemaId(), job.getTableId(), indexWithId);
                });

                return updateSchemaVersion(dc, job);
            case SCHEMA_WRITE_ONLY:
                withId = (TableDefinitionWithId) InfoSchemaService.root()
                    .getReplicaTable(job.getSchemaId(), job.getTableId(), 0);
                withId.getTableDefinition().setSchemaState(SCHEMA_WRITE_REORG);

                TableUtil.updateReplicaTable(job.getSchemaId(), job.getTableId(), withId);
                modifyColumnInfo.getNewCol().setSchemaState(SchemaState.SCHEMA_WRITE_REORG);
                job.setSchemaState(SchemaState.SCHEMA_WRITE_REORG);

                // handle index
                indexWithIdList = InfoSchemaService.root()
                    .getReplicaIndex(job.getSchemaId(), job.getTableId());
                indexWithIdList.forEach(indexObj -> {
                    TableDefinitionWithId indexWithId = (TableDefinitionWithId) indexObj;
                    indexWithId.getTableDefinition().setSchemaState(SCHEMA_WRITE_REORG);
                    TableUtil.updateReplicaTable(job.getSchemaId(), job.getTableId(), indexWithId);
                });
                return updateSchemaVersion(dc, job);
            case SCHEMA_WRITE_REORG:
                CommonId tableId = MapperImpl.MAPPER.idFrom(tableRes.getKey().getTableId());
                modifyColumnInfo.getNewCol().setSchemaState(SchemaState.SCHEMA_PUBLIC);

                withId = (TableDefinitionWithId) InfoSchemaService.root()
                    .getReplicaTable(job.getSchemaId(), job.getTableId(), 0);
                DingoCommonId replicaTableId = withId.getTableId();
                DingoErr dingoErr = DdlColumn.doReorgWorkForModifyCol(dc, job, tableId, withId, this);
                if (dingoErr.errorCode > 0) {
                    MetaService.root().dropRegionByTable(
                        Mapper.MAPPER.idFrom(replicaTableId), job.getId(), job.getRealStartTs(), false
                    );
                    InfoSchemaService.root().dropIndex(tableId.seq, replicaTableId.getEntityId());
                    LogUtils.info(log, "rollback drop replica table, priId:{}, replica id:{}",
                        tableId.seq, replicaTableId);
                    job.setState(JobState.jobStateCancelled);
                    job.setDingoErr(dingoErr);
                    updateSchemaVersion(dc, job);
                    return Pair.of(0L, dingoErr.errorMsg);
                }

                withId.getTableDefinition().setSchemaState(SCHEMA_PUBLIC);
                TableUtil.updateReplicaTable(job.getSchemaId(), job.getTableId(), withId);
                withId.setTableId(tableRes.getKey().getTableId());
                withId.getTableDefinition().setName(tableRes.getKey().getTableDefinition().getName());

                // do handle index
                indexWithIdList = InfoSchemaService.root().getReplicaIndex(job.getSchemaId(), job.getTableId());
                for (Object indexObj : indexWithIdList) {
                    TableDefinitionWithId indexWithId = (TableDefinitionWithId) indexObj;
                    DingoErr err = doModifyColumnIndex(indexWithId, job, tableId);
                    if (err != null && err.errorCode > 0) {
                        job.setDingoErr(err);
                        return Pair.of(0L, err.errorMsg);
                    }
                }
                // replace replicaTable to table
                try {
                    // to remove origin replica table definition
                    InfoSchemaService.root().dropIndex(tableId.seq, replicaTableId.getEntityId());
                    // remove old region
                    MetaService.root().dropRegionByTable(tableId, job.getId(), job.getRealStartTs(), false);
                } catch (Exception e) {
                    LogUtils.error(log, "drop replicaTable error", e);
                }

                if (job.isRollingback()) {
                    job.finishTableJob(JobState.jobStateRollbackDone, SchemaState.SCHEMA_PUBLIC);
                } else {
                    job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
                }
                return TableUtil.updateVersionAndTableInfos(dc, job, withId, true);
            default:
                job.setDingoErr(DingoErrUtil.newInternalErr(
                    ErrInvalidDDLState, "column", job.getSchemaState().toString()));
                error = job.getDingoErr().errorMsg;
        }
        return Pair.of(0L, error);
    }

    public Pair<Long, String> onRecoverSchema(DdlContext dc, DdlJob job) {
        // checkEnableGc
        String enableGcStr = InfoSchemaService.root().getGlobalVariables().get("enable_safe_point_update");
        boolean enableGc = Boolean.parseBoolean(enableGcStr);

        // get tableInfo
        job.decodeArgs();

        RecoverInfo recoverInfo = (RecoverInfo) job.getArgs().get(0);
        // schemaState none: save flg
        // write-only: disableGc -> checkSafePoint -> recoverTable -> updateVersionAndTableInfo
        // -> public -> finishTable
        String error;
        switch (job.getSchemaState()) {
            case SCHEMA_NONE:
                recoverInfo.setEnableGc(enableGc);
                job.setSchemaState(SchemaState.SCHEMA_WRITE_ONLY);
                return updateSchemaVersion(dc, job);
            case SCHEMA_WRITE_ONLY:
                if (enableGc) {
                    // disableGc
                    InfoSchemaService.root().putGlobalVariable("enable_safe_point_update", "0");
                }
                // recover table
                InfoSchemaService infoSchemaService
                    = new io.dingodb.store.service.InfoSchemaService(recoverInfo.getSnapshotTs());
                SchemaInfo schemaInfo = (SchemaInfo) infoSchemaService.getSchema(recoverInfo.getSchemaId());
                job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
                InfoSchemaService currentInfoService = InfoSchemaService.root();
                currentInfoService.createSchema(schemaInfo.getSchemaId(), schemaInfo);
                return updateSchemaVersion(dc, job);
            default:
                job.setDingoErr(DingoErrUtil.newInternalErr(
                    ErrInvalidDDLState, "schema", job.getSchemaState().toString()));
                error = job.getDingoErr().errorMsg;
        }
        return Pair.of(0L, error);
    }

    public static void finishRecoverTable(DdlJob ddlJob) {
        String error = ddlJob.decodeArgs();
        if (error != null) {
            return;
        }
        RecoverInfo recoverInfo = (RecoverInfo) ddlJob.getArgs().get(0);
        if (recoverInfo.isEnableGc()) {
            InfoSchemaService.root().putGlobalVariable("enable_safe_point_update", 1);
        }
    }

    public static boolean needModifyColWIthData(
        io.dingodb.common.table.ColumnDefinition oldColDef,
        io.dingodb.common.table.ColumnDefinition newColDef
    ) {
        if (newColDef.getTypeName().equals(oldColDef.getTypeName())) {
            //if (oldColDef.getTypeName().equalsIgnoreCase("DECIMAL")) {
                // add signed
            //    return oldColDef.getPrecision() != newColDef.getPrecision()
            //        || oldColDef.getScale() != newColDef.getScale();
            //}
            // if type is tiny/short/int/bigint -> = toUnsigned == originUnsigned
            //return oldColDef.getPrecision();
            return oldColDef.isNullable() != newColDef.isNullable();
        } else {
            return true;
        }
    }

    public DingoErr doModifyColumnIndex(
        TableDefinitionWithId withId, DdlJob job, CommonId tableId
    ) {
        DingoCommonId replicaTableId = withId.getTableId();
        LogUtils.info(log, "doModifyColumnIndex replicaId:{}, tableId:{}", replicaTableId, tableId);
        DingoErr dingoErr = DdlColumn.doReorgWorkForModifyIndexCol(
            DdlContext.INSTANCE, job, tableId, withId, this);
        if (dingoErr.errorCode > 0) {
            MetaService.root().dropRegionByTable(
                Mapper.MAPPER.idFrom(replicaTableId), job.getId(), job.getRealStartTs(), false
            );
            InfoSchemaService.root().dropIndex(tableId.seq, replicaTableId.getEntityId());
            job.setState(JobState.jobStateCancelled);
            return dingoErr;
        }

        withId.getTableDefinition().setSchemaState(SCHEMA_PUBLIC);
        String toIndexName = withId.getTableDefinition().getName();
        String originIndexName = toIndexName.substring(18);
        List<Object> indexList = InfoSchemaService.root().listIndex(job.getSchemaId(), job.getTableId());
        DingoCommonId originIndexId = indexList.stream()
            .map(obj -> (TableDefinitionWithId)obj)
            .filter(indexTable -> indexTable.getTableDefinition().getName().equals(originIndexName))
            .map(TableDefinitionWithId::getTableId)
            .findFirst().orElse(null);
        if (originIndexId == null) {
            return DingoErrUtil.newInternalErr("doModifyColumnIndex error, indexName:" + originIndexName);
        }

        withId.setTableId(originIndexId);
        withId.getTableDefinition().setName(originIndexName);
        // replace replicaTable to table
        try {
            // remove old region
            MetaService.root().dropRegionByTable(
                MapperImpl.MAPPER.idFrom(originIndexId), job.getId(), job.getRealStartTs(), false
            );
            // to remove origin definition
            InfoSchemaService.root().dropIndex(tableId.seq, originIndexId.getEntityId());
            TableUtil.updateVersionAndIndexInfos(DdlContext.INSTANCE, job, withId, false);
            return DingoErrUtil.normal();
        } catch (Exception e) {
            LogUtils.error(log, "drop replicaTable error", e);
            return DingoErrUtil.newInternalErr(e.getMessage());
        }
    }

    public Pair<Long, String> onRebaseAuto(DdlContext dc, DdlJob job) {
        String error = job.decodeArgs();
        if (error != null) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, error);
        }
        long autoInc = (long) job.getArgs().get(0);
        Pair<TableDefinitionWithId, String> tableRes = checkTableExistAndCancelNonExistJob(job, job.getSchemaId());
        if (tableRes.getValue() != null && tableRes.getKey() == null) {
            return Pair.of(0L, tableRes.getValue());
        }

        AutoIncrementService autoIncrementService = AutoIncrementService.INSTANCE;
        io.dingodb.server.executor.common.DingoCommonId dingoCommonId
            = new io.dingodb.server.executor.common.DingoCommonId(
            new CommonId(CommonId.CommonType.TABLE, job.getSchemaId(), job.getTableId())
        );
        long current = autoIncrementService.current(dingoCommonId);
        if (autoInc > current) {
            autoIncrementService.updateIncrement(dingoCommonId, autoInc);
        }
        job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
        return updateSchemaVersion(dc, job);
    }

    public Pair<Long, String> onResetAutoInc(DdlContext dc, DdlJob job) {
        job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
        return updateSchemaVersion(dc, job);
    }

    public Pair<Long, String> onRenameTable(DdlContext dc, DdlJob job) {
        String error = job.decodeArgs();
        if (error != null) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, error);
        }
        String toName = job.getArgs().get(0).toString();
        Pair<TableDefinitionWithId, String> tableRes = checkTableExistAndCancelNonExistJob(job, job.getSchemaId());
        if (tableRes.getValue() != null && tableRes.getKey() == null) {
            return Pair.of(0L, tableRes.getValue());
        }
        tableRes.getKey().getTableDefinition().setName(toName);
        job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
        return TableUtil.updateVersionAndTableInfos(dc, job, tableRes.getKey(), true);
    }

    public Pair<Long, String> onRenameIndex(DdlContext dc, DdlJob job) {
        String error = job.decodeArgs();
        if (error != null) {
            job.setState(JobState.jobStateCancelled);
            return Pair.of(0L, error);
        }
        String toName = job.getArgs().get(1).toString();
        String originName = job.getArgs().get(0).toString();
        Pair<TableDefinitionWithId, String> tableRes = checkTableExistAndCancelNonExistJob(job, job.getSchemaId());
        if (tableRes.getValue() != null && tableRes.getKey() == null) {
            return Pair.of(0L, tableRes.getValue());
        }
        List<Object> indexList = InfoSchemaService.root().listIndex(job.getSchemaId(), job.getTableId());
        TableDefinitionWithId indexWithId = indexList.stream()
            .map(idxTable -> (TableDefinitionWithId)idxTable)
            .filter(idxTable ->
            idxTable.getTableDefinition().getName().endsWith(originName)
            || idxTable.getTableDefinition().getName().endsWith(originName.toUpperCase())).findFirst().orElse(null);
        if (indexWithId == null) {
            job.setDingoErr(DingoErrUtil.newInternalErr(ErrKeyDoesNotExist, originName, job.getTableName()));
            return Pair.of(0L, job.getDingoErr().errorMsg);
        }
        toName = job.getTableName() + "." + toName;
        indexWithId.getTableDefinition().setName(toName);

        job.finishTableJob(JobState.jobStateDone, SchemaState.SCHEMA_PUBLIC);
        return TableUtil.updateVersionAndIndexInfos(dc, job, indexWithId, true);
    }

}
