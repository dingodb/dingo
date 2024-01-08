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
import io.dingodb.common.mysql.client.SessionVariableChange;
import io.dingodb.common.mysql.client.SessionVariableWatched;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionUtil;
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
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

@Slf4j
public class DingoConnection extends AvaticaConnection implements CalcitePrepare.Context{
    @Getter
    private final DingoParserContext context;

    private final Properties sessionVariables;

    private boolean autoCommit = true;

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

    public ITransaction createTransaction(boolean pessimistic) {
        if (transaction == null) {
            long startTs = TransactionManager.getStart_ts();
            this.transaction = TransactionManager.createTransaction(pessimistic, startTs,
                TransactionUtil.convertIsolationLevel(getClientInfo("transaction_isolation")));
            transaction.setTransactionConfig(sessionVariables);
        }
        return transaction;
    }

    public void cleanTransaction() throws SQLException {
        if(transaction != null) {
            if (!transaction.isAutoCommit()) {
                transaction = null;
                setAutoCommit(true);
            }
            transaction = null;
        }
    }

    public void beginTransaction(boolean pessimistic) throws SQLException{
        try {
            if(this.transaction != null) {
                // commit
                this.transaction.commit(getMeta().getJobManager());
            }
        } catch (Exception e) {
            log.info(e.getMessage(), e);
            throw new SQLException(e);
        } finally {
            if(this.transaction != null) {
                this.transaction.close();
                this.transaction = null;
            }
        }
        createTransaction(pessimistic);
        this.autoCommit = false;
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
            if(this.transaction != null) {
                // commit
                this.transaction.commit(getMeta().getJobManager());
            }
        } catch (Exception e) {
            log.info(e.getMessage(), e);
            throw new SQLException(e);
        } finally {
            if(this.transaction != null) {
                this.transaction.close();
                this.transaction = null;
            }
        }
        if(autoCommit == false) {
            createTransaction(false);
            this.autoCommit = false;
        }

    }

    @Override
    public boolean getAutoCommit() {
        return autoCommit;
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
    protected Meta.ExecuteResult prepareAndExecuteInternal(
        AvaticaStatement statement,
        String sql,
        long maxRowCount
    ) throws SQLException, NoSuchStatementException {
        return super.prepareAndExecuteInternal(statement, sql, maxRowCount);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        if (name.equalsIgnoreCase("transaction_isolation")) {
            if (transaction != null) {
                throw new RuntimeException("Transaction characteristics can't be changed while a transaction is in progress");
            }
            // optimistic transaction only support REPEATABLE-READ transaction isolation
            if (value.equalsIgnoreCase("READ-COMMITTED") && getClientInfo("txn_mode").equalsIgnoreCase("optimistic")) {
                return;
            }
        }
        sessionVariables.setProperty(name, value);
        if (name.equalsIgnoreCase("autocommit")) {
            try {
                setAutoCommit(value.equalsIgnoreCase("on") ? true : false);
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
