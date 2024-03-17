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
import io.dingodb.calcite.schema.DingoRootSchema;
import io.dingodb.common.CommonId;
import io.dingodb.common.mysql.client.SessionVariableChange;
import io.dingodb.common.mysql.client.SessionVariableWatched;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionUtil;
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
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class DingoConnection extends AvaticaConnection implements CalcitePrepare.Context{
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

    protected DingoConnection(
        DingoDriver driver,
        AvaticaFactory factory,
        String url,
        Properties info
    ) {
        super(driver, factory, url, info);
        String defaultSchema = info.getProperty("defaultSchema");
        if (defaultSchema == null) {
            defaultSchema = DingoRootSchema.DEFAULT_SCHEMA_NAME;
        }
        if (log.isInfoEnabled()) {
            log.info("Connection url = {}, properties = {}, default schema = {}.", url, info, defaultSchema);
        }
        context = new DingoParserContext(defaultSchema, info);
        sessionVariables = new Properties();
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
                log.error("Lose table lock, will close connection.", e);
                try {
                    close();
                } catch (SQLException ex) {
                    log.error("Lose table lock, close connection error.", e);
                }
            }
        });
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
            if (type == TransactionType.OPTIMISTIC && txIsolation.equalsIgnoreCase("READ-COMMITTED")) {
                throw new RuntimeException("Optimistic transaction only support" +
                    " read committed transaction isolation level");
            }
            this.transaction = TransactionManager.createTransaction(type, startTs,
                TransactionUtil.convertIsolationLevel(txIsolation));
            transaction.setTransactionConfig(sessionVariables);
            transaction.setAutoCommit(autoCommit);
        }
        return transaction;
    }

    public synchronized void cleanTransaction() throws SQLException {
        if (transaction != null) {
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
            log.info(e.getMessage(), e);
            throw new SQLException(e);
        } finally {
            getMeta().cleanTransaction();
        }
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
            log.info(e.getMessage(), e);
            throw new SQLException(e);
        } finally {
            getMeta().cleanTransaction();
        }
        if (!autoCommit) {
            createTransaction("pessimistic".equalsIgnoreCase(getClientInfo("txn_mode")) ?
                TransactionType.PESSIMISTIC : TransactionType.OPTIMISTIC, false);
            this.autoCommit = false;
        }
    }

    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    @Override
    public void close() throws SQLException {
        super.close();
        getMeta().cleanTransaction();
        unlockTables();
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
        } catch (Exception e) {
            throw e;
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
        this.command = sql;
        this.commandStartTime = System.currentTimeMillis();
        try {
            Meta.ExecuteResult result = super.prepareAndExecuteInternal(statement, sql, maxRowCount);
            return result;
        } catch (Exception e) {
            this.commandStartTime = 0;
            throw e;
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        if (name.equalsIgnoreCase("transaction_isolation")
            || name.equalsIgnoreCase("onetime_transaction_isolation")) {
            if (transaction != null) {
                throw new RuntimeException("Transaction characteristics can't be changed while a transaction is in progress");
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
    public String getClientInfo(String name) {
        return sessionVariables.getProperty(name);
    }

    @Override
    public Properties getClientInfo() {
        return sessionVariables;
    }

    @Override
    public void setClientInfo(Properties properties) {
        sessionVariables.putAll(properties);
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
}
