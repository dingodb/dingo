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
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.schema.RootSnapshotSchema;
import io.dingodb.common.CommonId;
import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.mysql.client.SessionVariableChange;
import io.dingodb.common.mysql.client.SessionVariableWatched;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.profile.CommitProfile;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.transaction.api.LockType;
import io.dingodb.transaction.api.TableLock;
import io.dingodb.transaction.api.TableLockService;
import io.dingodb.tso.TsoService;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.RelRunner;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.dingodb.exec.transaction.base.TransactionType.NONE;

@Slf4j
public class DingoConnection extends AvaticaConnection implements CalcitePrepare.Context {

    @Getter
    private final DingoParserContext context;

    private final Properties sessionVariables;

    private boolean autoCommit = true;

    private String oneTimeTxIsolation;

    @Getter
    private List<CommonId> lockTables;
    private CompletableFuture<Void> unlockFuture;

    @Getter
    @Setter
    private volatile String command;

    @Getter
    @Setter
    private volatile long commandStartTime;

    @Setter
    @Getter
    private ITransaction transaction;

    @Getter
    private CommitProfile commitProfile;

    @Getter
    @Setter
    private long pointTs;

    @Getter
    private Map<Long, Long> mdlLockJobMap = new ConcurrentHashMap<>();

    protected DingoConnection(
        DingoDriver driver,
        AvaticaFactory factory,
        String url,
        Properties info
    ) {
        super(driver, factory, url, info);
        String defaultSchema = info.getProperty("defaultSchema");
        if (defaultSchema == null) {
            defaultSchema = RootSnapshotSchema.DEFAULT_SCHEMA_NAME;
        }
        LogUtils.info(log, "DingoConnection:" + id);
        info.put("connId", id);
        LogUtils.trace(log, "Connection url = {}, properties = {}, default schema = {}.", url, info, defaultSchema);
        sessionVariables = new Properties();
        context = new DingoParserContext(defaultSchema, info, sessionVariables);
        String user = info.getProperty("user");
        String host  = info.getProperty("host");
        if (user != null && host != null) {
            sessionVariables.setProperty("@user", user);
            sessionVariables.setProperty("@host", host);
        }
        try {
            InfoSchemaService infoSchemaService = InfoSchemaService.root();
            Map<String, String> globalVariableMap = infoSchemaService.getGlobalVariables();
            Properties globalProp = ScopeVariables.putAllGlobalVar(globalVariableMap);
            this.setClientInfo(globalProp);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        }
    }

    public DingoMeta getMeta() {
        return (DingoMeta) meta;
    }

    public synchronized void lockTables(List<CommonId> tableIds, LockType type) {
        unlockTables();
        try {
            commit();
        } catch (SQLException e) {
            throw ExceptionUtils.toRuntime(e);
        }
        int ttl = Optional.mapOrGet(getClientInfo("lock_wait_timeout"), Integer::parseInt, () -> 50);
        int start = Utils.currentSecond();
        long lockTs = TsoService.getDefault().tso();
        CompletableFuture<Void> unlockFuture = new CompletableFuture<>();
        for (CommonId tableId : tableIds) {
            CompletableFuture<Boolean> lockFuture = new CompletableFuture<>();
            TableLockService.getDefault().lock(TableLock.builder()
                .lockTs(lockTs)
                .currentTs(lockTs)
                .type(type)
                .tableId(tableId)
                .lockFuture(lockFuture)
                .unlockFuture(unlockFuture)
                .build());
            int nextTtl = (start + ttl) - Utils.currentSecond();
            if (nextTtl < 0) {
                unlockFuture.complete(null);
                throw new RuntimeException("Lock wait timeout exceeded.");
            }

            try {
                lockFuture.get(nextTtl, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                lockFuture.cancel(true);
                unlockFuture.complete(null);
                throw new RuntimeException("Lock wait timeout exceeded.");
            } catch (Exception e) {
                lockFuture.cancel(true);
                unlockFuture.complete(null);
                throw ExceptionUtils.toRuntime(e);
            }
        }
        if (type == LockType.TABLE || type == LockType.RANGE) {
            this.lockTables = tableIds;
            this.unlockFuture = unlockFuture;
        }
        unlockFuture.whenComplete((r, e) -> {
            if (e != null) {
                LogUtils.error(log, "Lose table lock, will close connection.", e);
                try {
                    close();
                } catch (SQLException ex) {
                    LogUtils.error(log, "Lose table lock, close connection error.", e);
                }
            }
        });
    }

    @Override
    public void commit() throws SQLException {
        this.mdlLockJobMap.clear();
        super.commit();
    }

    @Override
    public void rollback() throws SQLException {
        this.mdlLockJobMap.clear();
        super.rollback();
    }

    public void unlockTables() {
        if (unlockFuture != null) {
            unlockFuture.complete(null);
        }
        unlockFuture = null;
        lockTables = null;
    }

    public synchronized ITransaction createTransaction(TransactionType type, boolean autoCommit) {
        if (transaction == null) {
            long startTs = TransactionManager.getStartTs();
            String txIsolation;
            if (oneTimeTxIsolation != null) {
                txIsolation = oneTimeTxIsolation;
            } else {
                txIsolation = getClientInfo("transaction_isolation");
            }
            if (type == TransactionType.OPTIMISTIC && "READ-COMMITTED".equalsIgnoreCase(txIsolation)) {
                throw new RuntimeException("Optimistic transaction only support"
                    + " read committed transaction isolation level");
            }
            if (!this.getContext().keyExists("sql_log")) {
                LogUtils.info(log, "create transaction, startTs:{}, type:{}, txIsolation:{}, autoCommit:{}",
                    startTs, type, txIsolation, autoCommit);
            }
            this.transaction = TransactionManager.createTransaction(type, startTs,
                TransactionUtil.convertIsolationLevel(txIsolation));
            transaction.setTransactionConfig(sessionVariables);
            transaction.setAutoCommit(autoCommit);
            if (pointTs > 0) {
                InfoSchema pointIs = DdlService.root().getPointIs(pointTs);
                getContext().getRootSchema().initTxnSnapshotMeta(transaction.getTxnId(), pointIs);
                transaction.setIs(pointIs);
                pointTs = 0;
            } else {
                InfoSchema is = getContext().getRootSchema().initTxn(transaction.getTxnId());
                transaction.setIs(is);
            }
            this.getContext().setAutoCommit(autoCommit);
        }
        return transaction;
    }

    public ITransaction initTransaction(boolean isTxn, boolean once) {
        if (!isTxn && once) {
            cleanTransaction();
            transaction = createTransaction(NONE, getAutoCommit());
            return transaction;
        }
        return transaction;
    }

    public synchronized void cleanTransaction() {
        if (transaction != null) {
            if (TransactionManager.getTransaction(transaction.getTxnId()) != null) {
                TransactionManager.unregister(transaction.getTxnId());
            }
            this.commitProfile = transaction.getCommitProfile();
            transaction = null;
            oneTimeTxIsolation = null;
        }
    }

    public void beginTransaction(boolean pessimistic) throws SQLException {
        try {
            if (this.transaction != null) {
                // commit
                this.transaction.commit(getMeta().getJobManager());
            }
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            throw new SQLException(e);
        } finally {
            getMeta().cleanTransaction();
        }
        LogUtils.debug(log, "begin transaction...");
        createTransaction(pessimistic ? TransactionType.PESSIMISTIC : TransactionType.OPTIMISTIC, false);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.checkOpen();
        this.meta.connectionSync(this.handle, (new ConnectionPropertiesImpl()).setAutoCommit(autoCommit));
        if (this.autoCommit == autoCommit) {
            // true==true and false == false: nothing
            return;
        }
        this.autoCommit = autoCommit;
        try {
            if (this.transaction != null) {
                // commit
                this.transaction.commit(getMeta().getJobManager());
            }
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            throw new SQLException(e);
        } finally {
            getMeta().cleanTransaction();
        }
        if (!autoCommit) {
            createTransaction("pessimistic".equalsIgnoreCase(getClientInfo("txn_mode"))
                ? TransactionType.PESSIMISTIC : TransactionType.OPTIMISTIC, false);
            this.autoCommit = false;
        }
    }

    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    @Override
    public void close() throws SQLException {
        LogUtils.info(log, "call dingoConnection close..., id:" + this.id);
        super.close();
        try {
            // close call cancel, ensure data consistency and program security and controllability
            if (this.statementMap != null) {
                this.statementMap.forEach((k, v) -> {
                    try {
                        v.cancel();
                    } catch (SQLException e) {
                        LogUtils.error(log, e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                });
            }
            if (transaction != null) {
                LogUtils.info(log, "call dingoConnection close..., txnId:" + transaction.getTxnId());
                transaction.cancel();
            }
        } finally {
            getMeta().cleanTransaction();
            unlockTables();
        }
    }

    @NonNull
    public AvaticaStatement getStatement(Meta.@NonNull StatementHandle sh) throws NoSuchStatementException {
        AvaticaStatement statement = statementMap.get(sh.id);
        if (statement != null) {
            return statement;
        }
        throw new NoSuchStatementException(sh);
    }

    public AvaticaResultSet newResultSet(
        AvaticaStatement statement,
        Meta.Signature sig,
        Meta.Frame firstFrame,
        String sql
    ) throws SQLException {
        final TimeZone timeZone = getTimeZone();
        return factory.newResultSet(statement, new QueryState(sql), sig, timeZone, firstFrame);
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
        return context.getTypeFactory();
    }

    @Override
    public CalciteSchema getRootSchema() {
        return context.getRootSchema();
    }

    @Override
    public CalciteSchema getMutableRootSchema() {
        return context.getRootSchema();
    }

    @Override
    public List<String> getDefaultSchemaPath() {
        return ImmutableList.of(context.getDefaultSchemaName());
    }

    @Override
    public CalcitePrepare.SparkHandler spark() {
        return null;
    }

    @Override
    public DataContext getDataContext() {
        return new DingoDataContext(this);
    }

    @Override
    public @Nullable List<String> getObjectPath() {
        return null;
    }

    @Override
    public RelRunner getRelRunner() {
        return null;
    }

    @Override
    public CalciteConnectionConfig config() {
        return new CalciteConnectionConfigImpl(info);
    }

    @Override
    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        checkOpen();
        try {
            this.command = sql;
            final Meta.StatementHandle h = meta.prepare(handle, sql, -1);
            return factory.newPreparedStatement(
                this,
                h,
                h.signature,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability
            );
        } catch (RuntimeException e) {
            LogUtils.error(log, e.getMessage(), e);
            throw ExceptionUtils.toSql(e);
        }
    }

    @Override
    protected ResultSet executeQueryInternal(AvaticaStatement statement,
                                             Meta.Signature signature,
                                             Meta.Frame firstFrame,
                                             QueryState state,
                                             boolean isUpdate) throws SQLException {
        try {
            return super.executeQueryInternal(statement, signature, firstFrame, state, isUpdate);
        } finally {
            this.commandStartTime = 0;
        }
    }

    @Override
    protected Meta.ExecuteResult prepareAndExecuteInternal(
        AvaticaStatement statement,
        String sql,
        long maxRowCount
    ) throws SQLException, NoSuchStatementException {
        if (sql.length() > 1000) {
            this.command = sql.substring(0, 1000);
        } else {
            this.command = sql;
        }
        this.commandStartTime = System.currentTimeMillis();
        try {
            return super.prepareAndExecuteInternal(statement, sql, maxRowCount);
        } catch (Exception e) {
            this.commandStartTime = 0;
            throw e;
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        if ("metric_log_enable".equalsIgnoreCase(name)) {
            return;
        }
        if (name.equalsIgnoreCase("transaction_isolation")
            || name.equalsIgnoreCase("onetime_transaction_isolation")) {
            if (transaction != null) {
                throw new RuntimeException(
                    "Transaction characteristics can't be changed while a transaction is in progress"
                );
            }
            if (name.startsWith("onetime_transaction_isolation")) {
                oneTimeTxIsolation = value;
                return;
            }
        }
        sessionVariables.setProperty(name, value);
        if (name.equalsIgnoreCase("autocommit")) {
            try {
                setAutoCommit(value.equalsIgnoreCase("on"));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        SessionVariableWatched.getInstance().notifyObservers(
            SessionVariableChange.builder().id(id).name(name).value(value).build()
        );
    }

    @Override
    public void setClientInfo(Properties properties) {
        sessionVariables.putAll(properties);
        sessionVariables.remove("metric_log_enable");
        autoCommit = !("off".equalsIgnoreCase(getClientInfo("autocommit")));
        if (properties.containsKey("wait_timeout")) {
            String value = (String) properties.get("wait_timeout");
            SessionVariableWatched.getInstance().notifyObservers(
                SessionVariableChange.builder().id(id).name("wait_timeout").value(value).build()
            );
        }
        if (properties.containsKey("interactive_timeout")) {
            String value = (String) properties.get("interactive_timeout");
            SessionVariableWatched.getInstance().notifyObservers(
                SessionVariableChange.builder().id(id).name("interactive_timeout").value(value).build()
            );
        }
    }

    @Override
    public String getClientInfo(String name) {
        return sessionVariables.getProperty(name);
    }

    @Override
    public Properties getClientInfo() {
        return sessionVariables;
    }

    static class DingoDataContext implements DataContext {
        private final DingoConnection connection;

        DingoDataContext(DingoConnection connection) {
            this.connection = connection;
        }

        @Override
        public @Nullable SchemaPlus getRootSchema() {
            CalciteSchema rootSchema = connection.context.getRootSchema();
            return rootSchema == null ? null : rootSchema.plus();
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return connection.context.getTypeFactory();
        }

        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }

        @Override
        public @Nullable Object get(String name) {
            return connection.getContext().getOption(name);
        }
    }

    public String toString() {
        return id;
    }

    @Override
    public String getSchema() {
        if (context.getUsedSchema() != null) {
            return context.getUsedSchema().getName();
        }
        return "dingo";
    }

    public void removeLockDDLJobs(Map<Long, Long> jobsVerMap, Map<Long, String> jobsIdsMap) {
        Map<Long, Long> relatedTableForMdl = this.context.getRootSchema().getRelatedTableForMdl();
        //LogUtils.info(log, "[ddl] check mdl, rootCalciteSchema:{}, mdl size:{}",
        //    this.context.getRootSchema(), relatedTableForMdl.size());
        for (Map.Entry<Long, Long> useRelated : relatedTableForMdl.entrySet()) {
            Long tableId = useRelated.getKey();
            long useSchemaVer = useRelated.getValue();
            Iterator<Map.Entry<Long, Long>> jobVerIterator = jobsVerMap.entrySet().iterator();
            while (jobVerIterator.hasNext()) {
                Map.Entry<Long, Long> jobIdVerEntry = jobVerIterator.next();
                long jobId = jobIdVerEntry.getKey();
                long ver = jobIdVerEntry.getValue();
                Map<Long, Long> ids = str2LongMap(jobsIdsMap.get(jobId));
                if (ids.containsKey(tableId)) {
                    if (useSchemaVer < ver) {
                        jobVerIterator.remove();
                        mdlLockJobMap.put(jobId, jobId);
                        if (DdlUtil.timeOutError.get()) {
                            LogUtils.info(log, "[ddl] conn remove mdl lock,jobId:{}, use ver:{}, "
                                + "ver:{}, tableId:{}", jobId, useSchemaVer, ver, tableId);
                        }
                    } else {
                        LogUtils.debug(log, "[ddl] conn remove ddl, but ver is newest,jobId:{}, use ver:{},"
                            + "ver:{}, tableId:{}", jobId, useSchemaVer, ver, tableId);
                    }
                }
            }
        }
    }

    public static Map<Long, Long> str2LongMap(String ids) {
        String[] strs = ids.split(",");
        Map<Long, Long> res = new HashMap<>();
        for (String str : strs) {
            res.put(Long.parseLong(str), 0L);
        }
        return res;
    }
}
