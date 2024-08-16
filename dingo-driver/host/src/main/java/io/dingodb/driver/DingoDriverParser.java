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

package io.dingodb.driver;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DingoParser;
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.DingoRelOptTable;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.grammar.ddl.DingoSqlCreateTable;
import io.dingodb.calcite.grammar.ddl.SqlCommit;
import io.dingodb.calcite.grammar.ddl.SqlRollback;
import io.dingodb.calcite.operation.DdlOperation;
import io.dingodb.calcite.operation.DmlOperation;
import io.dingodb.calcite.operation.KillConnection;
import io.dingodb.calcite.operation.KillQuery;
import io.dingodb.calcite.operation.Operation;
import io.dingodb.calcite.operation.QueryOperation;
import io.dingodb.calcite.operation.ShowProcessListOperation;
import io.dingodb.calcite.rel.AutoIncrementShuttle;
import io.dingodb.calcite.rel.DingoBasicCall;
import io.dingodb.calcite.rel.DingoDocument;
import io.dingodb.calcite.rel.DingoVector;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.SqlUtil;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.ProcessInfo;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.audit.DingoAudit;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.profile.CommitProfile;
import io.dingodb.common.profile.ExecProfile;
import io.dingodb.common.profile.PlanProfile;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.exception.TaskFinException;
import io.dingodb.exec.fin.ErrorType;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.exception.LockWaitException;
import io.dingodb.transaction.api.LockType;
import io.dingodb.transaction.api.TableLock;
import io.dingodb.transaction.api.TableLockService;
import io.dingodb.tso.TsoService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static io.dingodb.exec.transaction.base.TransactionType.NONE;

@Slf4j
public final class DingoDriverParser extends DingoParser {
    private final DingoConnection connection;
    @Getter
    private boolean inTransaction;
    @Getter
    private final PlanProfile planProfile;
    @Getter
    private ExecProfile execProfile;
    @Getter
    private CommitProfile commitProfile;
    @Getter
    private final DingoAudit dingoAudit;

    public DingoDriverParser(@NonNull DingoConnection connection) {
        super(connection.getContext());
        this.connection = connection;
        this.planProfile = new PlanProfile();
        this.dingoAudit = new DingoAudit(IsolationLevel.InvalidIsolationLevel.name(), TransactionType.NONE.name());
    }

    private static RelDataType makeStruct(RelDataTypeFactory typeFactory, @NonNull RelDataType type) {
        if (type.isStruct()) {
            return type;
        }
        return typeFactory.builder().add("$0", type).build();
    }

    @NonNull
    private static List<ColumnMetaData> getColumnMetaDataList(
        JavaTypeFactory typeFactory,
        @NonNull RelDataType jdbcType,
        List<? extends @Nullable List<String>> originList
    ) {
        List<RelDataTypeField> fieldList = jdbcType.getFieldList();
        final List<ColumnMetaData> columns = new ArrayList<>(fieldList.size());
        for (int i = 0; i < fieldList.size(); ++i) {
            RelDataTypeField field = fieldList.get(i);
            if (field.getName().equalsIgnoreCase("_rowid")) {
                continue;
            }
            columns.add(metaData(
                typeFactory,
                columns.size(),
                field.getName(),
                field.getType(),
                originList.get(i)
            ));
        }
        return columns;
    }

    public static ColumnMetaData.AvaticaType avaticaType(
        @NonNull JavaTypeFactory typeFactory,
        @NonNull RelDataType type
    ) {
        SqlTypeName typeName = type.getSqlTypeName();
        switch (typeName) {
            case ARRAY:
            case MULTISET:
                return ColumnMetaData.array(
                    avaticaType(typeFactory, Objects.requireNonNull(type.getComponentType())),
                    type.getSqlTypeName().getName(),
                    ColumnMetaData.Rep.of(typeFactory.getJavaClass(type))
                );
            default:
                return ColumnMetaData.scalar(
                    type.getSqlTypeName().getJdbcOrdinal(),
                    type.getSqlTypeName().getName(),
                    ColumnMetaData.Rep.of(typeFactory.getJavaClass(type))
                );
        }
    }

    @NonNull
    private static ColumnMetaData metaData(
        @NonNull JavaTypeFactory typeFactory,
        int ordinal,
        String fieldName,
        @NonNull RelDataType type,
        @Nullable List<String> origins
    ) {
        ColumnMetaData.AvaticaType avaticaType = avaticaType(typeFactory, type);
        return new ColumnMetaData(
            ordinal,
            false,
            true,
            false,
            false,
            type.isNullable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls,
            true,
            type.getPrecision(),
            fieldName,
            origin(origins, 0),
            origin(origins, 2),
            type.getPrecision(),
            type.getScale(),
            origin(origins, 1),
            null,
            avaticaType,
            true,
            false,
            false,
            avaticaType.id == SqlType.FLOAT.id ? "java.lang.Float" : avaticaType.columnClassName()
        );
    }

    private static @Nullable String origin(@Nullable List<String> origins, int offsetFromEnd) {
        return origins == null || offsetFromEnd >= origins.size()
            ? null : origins.get(origins.size() - 1 - offsetFromEnd);
    }

    @NonNull
    private static List<AvaticaParameter> createParameterList(@NonNull RelDataType parasType) {
        List<RelDataTypeField> fieldList = parasType.getFieldList();
        final List<AvaticaParameter> parameters = new ArrayList<>(fieldList.size());
        for (RelDataTypeField field : fieldList) {
            RelDataType type = field.getType();
            parameters.add(
                new AvaticaParameter(
                    false,
                    type.getPrecision(),
                    type.getScale(),
                    type.getSqlTypeName().getJdbcOrdinal(),
                    type.getSqlTypeName().toString(),
                    Object.class.getName(),
                    field.getName()));
        }
        return parameters;
    }

    @Nonnull
    public Meta.Signature parseQuery(
        JobManager jobManager,
        long jobSeqId,
        String sql,
        boolean prepare
    ) {
        SqlNode sqlNode;
        try {
            sqlNode = parse(sql);
            if (sqlNode instanceof DingoSqlCreateTable) {
                ((DingoSqlCreateTable) sqlNode).setOriginalCreateSql(sql);
            }
        } catch (SqlParseException e) {
            throw ExceptionUtils.toRuntime(e);
        }
        syntacticSugar(sqlNode);
        planProfile.endParse();
        JavaTypeFactory typeFactory = connection.getTypeFactory();
        final Meta.CursorFactory cursorFactory = Meta.CursorFactory.ARRAY;
        planProfile.setStmtType(sqlNode.getKind().lowerName);

        // for compatible mysql protocol
        MysqlSignature mysqlSignature = getMysqlSignature(SqlUtil.checkSql(sqlNode, sql), sqlNode, typeFactory, cursorFactory);
        if (mysqlSignature != null) {
            return mysqlSignature;
        }

        if (sqlNode.getKind().belongsTo(SqlKind.DDL)) {
            planProfile.end();
            DingoDdlVerify.verify(sqlNode, connection);
            execProfile = new ExecProfile("DDL");
            Integer retry = Optional.mapOrGet(DingoConfiguration.instance().find("retry", int.class), __ -> __, () -> 30);
            while (retry-- > 0) {
                try {
                    final DdlExecutor ddlExecutor = PARSER_CONFIG.parserFactory().getDdlExecutor();
                    ddlExecutor.executeDdl(connection, sqlNode);
                    break;
                } catch (IllegalArgumentException e) {
                    // Method not found: execute([class org.apache.calcite.sql.ddl.SqlCreateTable,org.apache.calcite.jdbc.CalcitePrepare$Context])
                    LogUtils.error(log, e.getMessage(), e);
                    if (!e.getMessage().startsWith("Method not found: execute") || retry <= 0) {
                        throw e;
                    }
                } catch (RuntimeException e) {
                    // java.lang.RuntimeException: While invoking method 'public void io.dingodb.calcite.DingoDdlExecutor.execute(org.apache.calcite.sql.ddl.SqlCreateTable,org.apache.calcite.jdbc.CalcitePrepare$Context)'
                    LogUtils.error(log, e.getMessage(), e);
                    if (!(sqlNode instanceof DingoSqlCreateTable) || retry <= 0
                        || !e.getMessage().startsWith("While invoking method")) {
                        throw e;
                    }
                }
            }
            execProfile.end();
            return new DingoSignature(
                ImmutableList.of(),
                SqlUtil.checkSql(sqlNode, sql),
                Meta.CursorFactory.OBJECT,
                Meta.StatementType.OTHER_DDL,
                null,
                null
            );
        }

        SqlExplain explain = null;
        if (sqlNode.getKind().equals(SqlKind.EXPLAIN)) {
            assert sqlNode instanceof SqlExplain;
            explain = (SqlExplain) sqlNode;
            sqlNode = explain.getExplicandum();
        }

        long startTs;
        CommonId txn_Id;
        boolean pessimisticTxn;
        ITransaction transaction;
        boolean newTxn = false;
        if (connection.getTransaction() != null) {
            transaction = connection.getTransaction();
            txn_Id = transaction.getTxnId();
        } else {
            // autocommit is true use current txn mode
            transaction = connection.createTransaction(
                "pessimistic".equalsIgnoreCase(connection.getClientInfo("txn_mode")) ?
                    TransactionType.PESSIMISTIC : TransactionType.OPTIMISTIC,
                connection.getAutoCommit());
            txn_Id = transaction.getTxnId();
            if (pointTs > 0) {
                transaction.setPointStartTs(pointTs);
            }
            newTxn = true;
        }
        startTs = transaction.getStartTs();
        SqlValidator validator = getSqlValidator();
        try {
            sqlNode = validator.validate(sqlNode);
        } catch (CalciteContextException e) {
            LogUtils.error(log, "Parse and validate error, sql: <[{}]>.", sql, e);
            throw ExceptionUtils.toRuntime(e);
        }
        planProfile.endValidator();
        Meta.StatementType statementType;
        RelDataType type;
        switch (sqlNode.getKind()) {
            case INSERT:
            case DELETE:
            case UPDATE:
                statementType = Meta.StatementType.IS_DML;
                type = RelOptUtil.createDmlRowType(sqlNode.getKind(), typeFactory);
                break;
            default:
                statementType = Meta.StatementType.SELECT;
                type = validator.getValidatedNodeType(sqlNode);
                break;
        }
        RelDataType jdbcType = makeStruct(typeFactory, type);
        List<List<String>> originList = validator.getFieldOrigins(sqlNode);
        final List<ColumnMetaData> columns = getColumnMetaDataList(typeFactory, jdbcType, originList);

        final RelRoot relRoot = convert(sqlNode, false);
        RelNode relNode = optimize(relRoot.rel);
        planProfile.endOptimize();
        markAutoIncForDml(relNode);
        Location currentLocation = MetaService.root().currentLocation();
        RelDataType parasType = validator.getParameterRowType(sqlNode);
        Set<RelOptTable> tables = useTables(relNode, sqlNode);
        boolean isTxn = checkEngine(sqlNode, tables, connection.getTransaction(), planProfile);
        transaction = connection.initTransaction(isTxn, newTxn);

        // get in transaction for mysql update/insert/delete res ok packet
        if (transaction.getType() != NONE) {
            inTransaction = true;
        }
        // mysql protocol dml response ok need in transaction flag

        pessimisticTxn = transaction.isPessimistic();
        if (pessimisticTxn) {
            transaction.setForUpdateTs(jobSeqId);
        }
        if (pessimisticTxn && transaction.getPrimaryKeyLock() == null && explain == null) {
            runPessimisticPrimaryKeyJob(jobSeqId, jobManager, transaction, sqlNode, relNode,
                currentLocation, DefinitionMapper.mapToDingoType(parasType),
                "on".equalsIgnoreCase(connection.getClientInfo("dingo_join_concurrency_enable")));
            jobSeqId = transaction.getForUpdateTs();
        }
        // Automatically submitted selection, no locks
        if (!(transaction.isAutoCommit() && sqlNode.getKind() == SqlKind.SELECT)) {
            try {
                lockTables(tables, startTs, jobSeqId, transaction.getFinishedFuture());
            } catch (Exception e) {
                LogUtils.error(log, e.getMessage(), e);
                throw e;
            }
        }
        String maxExecutionTimeStr = connection.getClientInfo("max_execution_time");
        maxExecutionTimeStr = maxExecutionTimeStr == null ? "0" : maxExecutionTimeStr;
        long maxTimeOut = Long.parseLong(maxExecutionTimeStr);
        Job job = jobManager.createJob(
            startTs, jobSeqId, txn_Id, DefinitionMapper.mapToDingoType(parasType), maxTimeOut,
            statementType == Meta.StatementType.SELECT
        );
        DingoJobVisitor.renderJob(
            job,
            relNode,
            currentLocation,
            true,
            transaction.getType() == NONE ? null : connection.getTransaction(),
            sqlNode.getKind(),
            "on".equalsIgnoreCase(connection.getClientInfo("dingo_join_concurrency_enable"))
        );
        if (explain != null) {
            statementType = Meta.StatementType.CALL;
            String logicalPlan = RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES);
            if (explain.getDetailLevel() == SqlExplainLevel.EXPPLAN_ATTRIBUTES) {
                return new DingoExplainSignature(
                    new ArrayList<>(Collections.singletonList(metaData(typeFactory, 0, "PLAN",
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.CHAR), null))),
                    sql,
                    createParameterList(parasType),
                    null,
                    cursorFactory,
                    statementType,
                    sqlNode.toString(),
                    logicalPlan,
                    job,
                    explain.getDetailLevel()
                );
            } else {
                ColumnMetaData colMeta1 = metaData(typeFactory, 0, "id",
                    new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.CHAR), null);
                ColumnMetaData colMeta2 = metaData(typeFactory, 1, "estRows",
                    new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DOUBLE), null);
                ColumnMetaData colMeta3 = metaData(typeFactory, 1, "task",
                    new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.CHAR), null);
                ColumnMetaData colMeta4 = metaData(typeFactory, 2, "accessObject",
                    new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.CHAR), null);
                ColumnMetaData colMeta5 = metaData(typeFactory, 3, "info",
                    new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.CHAR), null);
                List<ColumnMetaData> metaDataList = new ArrayList<>();
                metaDataList.add(colMeta1);
                metaDataList.add(colMeta2);
                metaDataList.add(colMeta3);
                metaDataList.add(colMeta4);
                metaDataList.add(colMeta5);
                return new ExplainSignature(metaDataList, sql, createParameterList(parasType),
                    null,
                    cursorFactory,
                    statementType, relNode);
            }
        }
        planProfile.endLock();
        return new DingoSignature(
            columns,
            sql,
            createParameterList(parasType),
            null,
            cursorFactory,
            statementType,
            job.getJobId(),
            sqlNode,
            relNode,
            parasType,
            planProfile.getTableList()
        );
    }

    @Nullable
    private MysqlSignature getMysqlSignature(String sql,
            SqlNode sqlNode,
            JavaTypeFactory typeFactory, Meta.CursorFactory cursorFactory) {
        if (compatibleMysql(sqlNode, planProfile)) {
            planProfile.end();
            DingoDdlVerify.verify(sqlNode, connection);
            Operation operation = convertToOperation(sqlNode, connection, connection.getContext());
            Meta.StatementType statementType;
            List<ColumnMetaData> columns = new ArrayList<>();
            if (sqlNode.getKind() == SqlKind.SELECT || sqlNode.getKind() == SqlKind.ORDER_BY) {
                this.execProfile = new ExecProfile("exec");
                QueryOperation queryOperation = (QueryOperation) operation;
                queryOperation.initExecProfile(execProfile);
                columns = queryOperation.columns().stream().map(column -> metaData(typeFactory, 0, column,
                    new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.CHAR), null))
                    .collect(Collectors.toList());
                statementType = Meta.StatementType.SELECT;
                if (queryOperation instanceof ShowProcessListOperation) {
                    ShowProcessListOperation processListOperation = (ShowProcessListOperation) queryOperation;
                    List<ProcessInfo> processInfoList
                      = getProcessInfoList(ExecutionEnvironment.INSTANCE.sessionUtil.connectionMap);
                    processListOperation.init(processInfoList);
                }
            } else if (sqlNode.getKind() == SqlKind.INSERT) {
                columns = ((DmlOperation)operation).columns(typeFactory);
                statementType = Meta.StatementType.IS_DML;
                this.execProfile = new ExecProfile("dml");
                ((DmlOperation) operation).doExecute(execProfile);
            } else {
                Map<String, Connection> connectionMap
                    = ExecutionEnvironment.INSTANCE.sessionUtil.connectionMap;
                if (operation instanceof KillConnection) {
                    KillConnection killConnection = (KillConnection) operation;
                    String threadId = killConnection.getThreadId();
                    if (connectionMap.containsKey(threadId)) {
                        killConnection.initConnection(connectionMap.get(threadId));
                    } else if (connectionMap.containsKey(killConnection.getMysqlThreadId())) {
                        killConnection.initConnection(
                            connectionMap.get(killConnection.getMysqlThreadId())
                        );
                    }
                } else if (operation instanceof KillQuery) {
                    KillQuery killQuery = (KillQuery) operation;
                    killQuery.init(connectionMap);
                }
                if (sqlNode instanceof SqlCommit) {
                    if (connection.getTransaction() != null) {
                        dingoAudit(connection.getTransaction());
                    }
                    ((DdlOperation)operation).execute();
                    this.commitProfile = connection.getCommitProfile();
                }  else if (sqlNode instanceof SqlRollback) {
                    if (connection.getTransaction() != null) {
                        dingoAudit(connection.getTransaction());
                    }
                    this.execProfile = new ExecProfile("other_ddl");
                    ((DdlOperation)operation).doExecute(this.execProfile);
                }else {
                    this.execProfile = new ExecProfile("other_ddl");
                    ((DdlOperation)operation).doExecute(this.execProfile);
                }
                statementType = Meta.StatementType.OTHER_DDL;
            }
            return new MysqlSignature(columns,
                sql,
                new ArrayList<>(),
                null,
                cursorFactory,
                statementType,
                operation);
        }
        return null;
    }

    private void dingoAudit(ITransaction transaction) {
        dingoAudit.setStartTs(transaction.getStartTs());
        dingoAudit.setForUpdateTs(transaction.isPessimistic() ? transaction.getForUpdateTs() : 0L);
        dingoAudit.setTransactionType(transaction.getType().name());
        dingoAudit.setTxIsolation(IsolationLevel.of(transaction.getIsolationLevel()).name());
        dingoAudit.setAutoCommit(transaction.isAutoCommit());
    }

    private void lockTables(
        Set<RelOptTable> tables, long startTs, long jobSeqId, CompletableFuture<Void> finishedFuture
    ) {
        if (!DingoConfiguration.instance().getVariable().getEnableTableLock() || true) {
            return;
        }
        List<RelOptTable> waitLocktableList = new ArrayList<>();
        if (connection.getLockTables() != null && !connection.getLockTables().isEmpty()) {
            for (RelOptTable table : tables) {
                if (!connection.getLockTables().contains(Objects.requireNonNull(table.unwrap(DingoTable.class)).getTableId())) {
                    waitLocktableList.add(table);
                    //throw new RuntimeException("Not lock table: " + table.getQualifiedName());
                }
            }
            return;
        } else {
            waitLocktableList.addAll(tables);
        }
        int ttl = Optional.mapOrGet(connection.getClientInfo("lock_wait_timeout"), Integer::parseInt, () -> 50);
        int start = Utils.currentSecond();
        for (RelOptTable table : waitLocktableList) {
            CompletableFuture<Boolean> lockFuture = new CompletableFuture<>();
            CompletableFuture<Void> unlockFuture = new CompletableFuture<>();
            TableLock lock = TableLock.builder()
                .lockTs(startTs)
                .currentTs(jobSeqId)
                .type(LockType.ROW)
                .tableId(Objects.requireNonNull(table.unwrap(DingoTable.class)).getTableId())
                .lockFuture(lockFuture)
                .unlockFuture(unlockFuture)
                .build();
            TableLockService.getDefault().lock(lock);
            LogUtils.info(log, "lockTables, jobSeqId:{}, startTs:{}, tableId:{}", jobSeqId, startTs, lock.tableId.toString());
            int nextTtl = (start + ttl) - Utils.currentSecond();
            if (nextTtl < 0) {
                lockFuture.cancel(true);
                unlockFuture.complete(null);
                unlockFuture.join();
                throw new RuntimeException(String.format("Lock wait timeout exceeded. tableId: %s.", lock.tableId.toString()));
            }
            try {
                lockFuture.get(nextTtl, TimeUnit.SECONDS);
                finishedFuture.whenComplete((v, t) -> unlockFuture.complete(null));
            } catch (TimeoutException e) {
                LogUtils.error(log, e.getMessage(), e);
                lockFuture.cancel(true);
                unlockFuture.complete(null);
                unlockFuture.join();
                throw new RuntimeException(String.format("Lock wait timeout exceeded. tableId: %s.", lock.tableId.toString()));
            } catch (Exception e) {
                LogUtils.error(log, e.getMessage(), e);
                lockFuture.cancel(true);
                unlockFuture.complete(null);
                unlockFuture.join();
                throw ExceptionUtils.toRuntime(e);
            }
        }
    }


    @Nonnull
    public Meta.Signature retryQuery(
        JobManager jobManager,
        String sql,
        SqlNode sqlNode,
        RelNode relNode,
        RelDataType parasType,
        List<ColumnMetaData> columns,
        boolean lockTable
    ) {
        final Meta.CursorFactory cursorFactory = Meta.CursorFactory.ARRAY;
        Meta.StatementType statementType;
        markAutoIncForDml(relNode);
        Location currentLocation = MetaService.root().currentLocation();
        Set<RelOptTable> tables = useTables(relNode, sqlNode);
        switch (sqlNode.getKind()) {
            case INSERT:
            case DELETE:
            case UPDATE:
                statementType = Meta.StatementType.IS_DML;
                break;
            default:
                statementType = Meta.StatementType.SELECT;
                break;
        }

        ITransaction transaction = connection.createTransaction(
            TransactionType.OPTIMISTIC,
            connection.getAutoCommit()
        );
        long startTs = transaction.getStartTs();
        long jobSeqId = TsoService.getDefault().tso();
        if (!(transaction.isAutoCommit() && sqlNode.getKind() == SqlKind.SELECT) && lockTable) {
            try {
                lockTables(tables, startTs, jobSeqId, transaction.getFinishedFuture());
            } catch (Exception e) {
                LogUtils.error(log, e.getMessage(), e);
                throw e;
            }
        }
        if (transaction.isPessimistic() && transaction.getPrimaryKeyLock() == null) {
            runPessimisticPrimaryKeyJob(jobSeqId, jobManager, transaction, sqlNode, relNode,
                currentLocation, DefinitionMapper.mapToDingoType(parasType),
                "on".equalsIgnoreCase(connection.getClientInfo("dingo_join_concurrency_enable")));
            jobSeqId = transaction.getForUpdateTs();
        }
        String maxExecutionTimeStr = connection.getClientInfo("max_execution_time");
        maxExecutionTimeStr = maxExecutionTimeStr == null ? "0" : maxExecutionTimeStr;
        long maxTimeOut = Long.parseLong(maxExecutionTimeStr);
        Job job = jobManager.createJob(
            startTs, jobSeqId, transaction.getTxnId(), DefinitionMapper.mapToDingoType(parasType), maxTimeOut,
            false
        );
        DingoJobVisitor.renderJob(
            job,
            relNode,
            currentLocation,
            true,
            transaction.getType() == NONE ? null : connection.getTransaction(),
            sqlNode.getKind(),
            "on".equalsIgnoreCase(connection.getClientInfo("dingo_join_concurrency_enable"))
        );
        return new DingoSignature(
            columns,
            sql,
            createParameterList(parasType),
            null,
            cursorFactory,
            statementType,
            job.getJobId(),
            sqlNode,
            relNode,
            parasType,
            null
        );
    }
    private static void runPessimisticPrimaryKeyJob(
        long jobSeqId,
        JobManager jobManager,
        ITransaction transaction,
        SqlNode sqlNode,
        RelNode relNode,
        Location currentLocation,
        DingoType dingoType,
        boolean isJoinConcurrency
    ) {
        Integer retry = Optional.mapOrGet(DingoConfiguration.instance().find("retry", int.class), __ -> __, () -> 30);
        while (retry-- > 0) {
            Job job = jobManager.createJob(transaction.getStartTs(), jobSeqId, transaction.getTxnId(), dingoType);
            DingoJobVisitor.renderJob(job, relNode, currentLocation, true, transaction, sqlNode.getKind(), isJoinConcurrency);
            try {
                Iterator<Object[]> iterator = jobManager.createIterator(job, null);
                while (iterator.hasNext()) {
                    iterator.next();
                }
                break;
            } catch (LockWaitException e) {
                transaction.rollBackPessimisticPrimaryLock(jobManager);
            } catch (TaskFinException e1) {
                transaction.rollBackPessimisticPrimaryLock(jobManager);
                if (!(e1.getErrorType().equals(ErrorType.LockWait))) {
                    LogUtils.error(log, e1.getMessage(), e1);
                    throw e1;
                }
            } catch (Throwable throwable) {
                LogUtils.error(log, throwable.getMessage(), throwable);
                transaction.rollBackPessimisticPrimaryLock(jobManager);
                throw ExceptionUtils.toRuntime(throwable);
            } finally {
                jobManager.removeJob(job.getJobId());
            }
        }
    }

    private static Set<RelOptTable> useTables(RelNode relNode, SqlNode sqlNode) {
        Set<RelOptTable> tables = RelOptUtil.findTables(relNode);
        if (sqlNode.getKind() == SqlKind.INSERT || sqlNode.getKind() == SqlKind.DELETE) {
            RelNode input = relNode.getInput(0).getInput(0);
            RelOptTable table = input.getTable();
            tables.add(table);
        }
        RelOptTable functionTable = findUserDefindedFunction(relNode);
        if (functionTable != null) {
            tables.add(functionTable);
        }
        return tables;
    }

    private static RelOptTable findUserDefindedFunction(RelNode relNode) {
        RelShuttleImpl relShuttle = new RelShuttleImpl() {
            @Override
            public RelNode visit(RelNode other) {
                if (other instanceof DingoVector || other instanceof  DingoDocument) {
                    return other;
                }
                if (!other.getInputs().isEmpty()) {
                    return visitChildren(other);
                } else {
                    return null;
                }
            }

            protected RelNode visitChildren(RelNode rel) {
                for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
                    rel = visitChild(input.e);
                    if (rel != null) {
                        return rel;
                    }
                }
                return rel;
            }

            private RelNode visitChild(RelNode child) {
                RelNode child2 = child.accept(this);
                if (child2 instanceof DingoVector) {
                    return child2;
                }
                return null;
            }
        };
        RelNode relNode1 = relNode.accept(relShuttle);
        if (relNode1 instanceof DingoVector) {
            DingoVector vector = (DingoVector) relNode1;
            return vector.getTable();
        } else if(relNode1 instanceof DingoDocument){
            DingoDocument document = (DingoDocument) relNode1;
            return document.getTable();
        }
        return null;
    }

    private static boolean checkEngine(SqlNode sqlNode,
                                       Set<RelOptTable> tables,
                                       ITransaction transaction,
                                       PlanProfile planProfile) {
        boolean isTxn = false;
//        boolean isNotTransactionTable = false;
        // for UT test
        if ((sqlNode.getKind() == SqlKind.SELECT || sqlNode.getKind() == SqlKind.DELETE) && tables.isEmpty()) {
            return false;
        }
        List<String> tableList = new ArrayList<>();
        planProfile.setTableList(tableList);
        String name = "";
        for (RelOptTable table : tables) {
            String engine = null;
            Table tableTarget;
            if (table instanceof RelOptTableImpl) {
                RelOptTableImpl relOptTable = (RelOptTableImpl) table;
                tableTarget = ((DingoTable) relOptTable.table()).getTable();
                engine = tableTarget.getEngine();
                List<String> fullName = relOptTable.getQualifiedName();
                if (fullName.size() == 3) {
                    name = fullName.get(1) + "." + fullName.get(2);
                    tableList.add(name);
                }
            } else if (table instanceof DingoRelOptTable) {
                DingoRelOptTable dingoRelOptTable = (DingoRelOptTable) table;
                tableTarget = ((DingoTable) dingoRelOptTable.table()).getTable();
                engine = tableTarget.getEngine();
                name = dingoRelOptTable.getSchemaName() + "." + dingoRelOptTable.getTableName();
                tableList.add(name);
            }
            if (engine == null || !engine.contains("TXN")) {
//                isNotTransactionTable = true;
            } else {
                isTxn = true;
            }
//            if (isTxn && isNotTransactionTable) {
//                throw new RuntimeException("Transactional tables cannot be mixed with non-transactional tables");
//            }
//            if (transaction != null && transaction.getType() != NONE && isNotTransactionTable) {
//                LogUtils.info(log, "transaction txnId is {}, table name is {}", transaction.getTxnId(), name);
//                throw new RuntimeException("Non-transaction tables cannot be used in transactions");
//            }
        }
        return isTxn;
    }

    private static void markAutoIncForDml(RelNode relNode) {
        try {
            relNode.accept(AutoIncrementShuttle.INSTANCE);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        }
    }

    private static List<ProcessInfo> getProcessInfoList(Map<String, Connection> connectionMap) {
        return connectionMap
            .entrySet()
            .stream()
            .map(entry -> {
                String type = "DINGO";
                String id = entry.getKey();
                if (id.startsWith("mysql:")) {
                    type = "MYSQL";
                    id = id.substring(6);
                }
                DingoConnection dingoConn = (DingoConnection) entry.getValue();
                String txnIdStr = "";
                if (dingoConn.getTransaction() != null && dingoConn.getTransaction().getTxnId() != null) {
                    txnIdStr = dingoConn.getTransaction().getTxnId().toString();
                }
                String info = dingoConn.getCommand();
                long commandStartTime = dingoConn.getCommandStartTime();
                String costTimeStr = null;
                String command = "query";
                if (commandStartTime == 0) {
                    command = "sleep";
                } else {
                    costTimeStr = String.valueOf(System.currentTimeMillis() - commandStartTime);
                }
                DingoParserContext context = dingoConn.getContext();
                ProcessInfo processInfo = new ProcessInfo();
                processInfo.setId(id);
                processInfo.setUser(context.getOption("user"));
                processInfo.setHost(context.getOption("host"));
                processInfo.setClient(context.getOption("client"));
                processInfo.setDb(context.getUsedSchema().getName());
                processInfo.setType(type);
                processInfo.setCommand(command);
                processInfo.setTime(costTimeStr);
                processInfo.setTxnIdStr(txnIdStr);
                try {
                    processInfo.setState(dingoConn.isClosed() ? "closed" : "open");
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                processInfo.setInfo(info);
                return processInfo;
            })
            .collect(Collectors.toList());
    }

    private void syntacticSugar(SqlNode sqlNode) {
        if (sqlNode instanceof SqlSelect) {
            SqlNodeList sqlNodes = ((SqlSelect) sqlNode).getSelectList();
            deepSugar(sqlNodes);
        } else if (sqlNode instanceof SqlOrderBy) {
            SqlOrderBy sqlOrderBy = (SqlOrderBy) sqlNode;
            if (sqlOrderBy.query instanceof SqlSelect) {
                SqlSelect sqlSelect = (SqlSelect) sqlOrderBy.query;
                deepSugar(sqlSelect.getSelectList());
            }
        }
    }

    public void deepSugar(List<SqlNode> sqlNodes) {
        if (sqlNodes == null) {
            return;
        }
        for (int i = 0; i < sqlNodes.size(); i ++) {
            SqlNode sqlNode1 = sqlNodes.get(i);
            if (sqlNode1 instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) sqlNode1;
                String opName = call.getOperator().getName();
                List<SqlNode> nodes = new ArrayList<>();
                if (opName.equalsIgnoreCase("database")
                    || opName.equalsIgnoreCase("schema")
                    || opName.equalsIgnoreCase("user")) {
                    sqlNodes.remove(i);
                    nodes.add(SqlLiteral.createCharString("DINGO", call.getParserPosition()));
                } else if (opName.equals("@") || opName.equals("@@")) {
                    sqlNodes.remove(i);
                    nodes.add(call.getOperandList().get(0));
                }
                if (nodes.isEmpty()) {
                    if (call.getOperandList() instanceof ImmutableList) {
                        sqlNodes.remove(i);
                        call = new DingoBasicCall(call);
                        sqlNodes.add(i, call);
                    }
                    deepSugar(call.getOperandList());
                    continue;
                }
                nodes.add(SqlLiteral.createCharString(connection.id, call.getParserPosition()));
                sqlNodes.add(i, new SqlBasicCall(call.getOperator(), nodes, call.getParserPosition()));
            }
        }
    }

}
