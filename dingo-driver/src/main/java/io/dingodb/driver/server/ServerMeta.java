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

package io.dingodb.driver.server;

import io.dingodb.driver.DingoConnection;
import io.dingodb.driver.DingoDriver;
import io.dingodb.driver.DingoMeta;
import io.dingodb.driver.DingoPreparedStatement;
import io.dingodb.driver.DingoStatement;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

// Only one meta instance exists in avatica server.
// On each request, create a local meta (which is light-weighted) to do the work.
@Slf4j
public class ServerMeta implements Meta {
    private final Map<String, DingoConnection> connectionMap = new ConcurrentHashMap<>();

    public ServerMeta() {
    }

    @Nonnull
    private Meta getConnectionMeta(@Nonnull ConnectionHandle ch) {
        DingoConnection connection = connectionMap.get(ch.id);
        return connection.getMeta();
    }

    @Override
    public Map<DatabaseProperty, Object> getDatabaseProperties(@Nonnull ConnectionHandle ch) {
        return getConnectionMeta(ch).getDatabaseProperties(ch);
    }

    @Override
    public MetaResultSet getTables(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern,
        List<String> typeList
    ) {
        return getConnectionMeta(ch).getTables(ch, catalog, schemaPattern, tableNamePattern, typeList);
    }

    @Override
    public MetaResultSet getColumns(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern,
        Pat columnNamePattern
    ) {
        return getConnectionMeta(ch).getColumns(ch, catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    @Override
    public MetaResultSet getSchemas(@Nonnull ConnectionHandle ch, String catalog, Pat schemaPattern) {
        return getConnectionMeta(ch).getSchemas(ch, catalog, schemaPattern);
    }

    @Override
    public MetaResultSet getCatalogs(@Nonnull ConnectionHandle ch) {
        return getConnectionMeta(ch).getCatalogs(ch);
    }

    @Override
    public MetaResultSet getTableTypes(@Nonnull ConnectionHandle ch) {
        return getConnectionMeta(ch).getTableTypes(ch);
    }

    @Override
    public MetaResultSet getProcedures(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat procedureNamePattern
    ) {
        return getConnectionMeta(ch).getProcedures(ch, catalog, schemaPattern, procedureNamePattern);
    }

    @Override
    public MetaResultSet getProcedureColumns(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat procedureNamePattern,
        Pat columnNamePattern
    ) {
        return getConnectionMeta(ch).getProcedureColumns(
            ch,
            catalog,
            schemaPattern,
            procedureNamePattern,
            columnNamePattern
        );
    }

    @Override
    public MetaResultSet getColumnPrivileges(
        @Nonnull ConnectionHandle ch,
        String catalog,
        String schema,
        String table,
        Pat columnNamePattern
    ) {
        return getConnectionMeta(ch).getColumnPrivileges(ch, catalog, schema, table, columnNamePattern);
    }

    @Override
    public MetaResultSet getTablePrivileges(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern
    ) {
        return getConnectionMeta(ch).getTablePrivileges(ch, catalog, schemaPattern, tableNamePattern);
    }

    @Override
    public MetaResultSet getBestRowIdentifier(
        @Nonnull ConnectionHandle ch,
        String catalog,
        String schema,
        String table,
        int scope,
        boolean nullable
    ) {
        return getConnectionMeta(ch).getBestRowIdentifier(ch, catalog, schema, table, scope, nullable);
    }

    @Override
    public MetaResultSet getVersionColumns(
        @Nonnull ConnectionHandle ch,
        String catalog,
        String schema,
        String table
    ) {
        return getConnectionMeta(ch).getVersionColumns(ch, catalog, schema, table);
    }

    @Override
    public MetaResultSet getPrimaryKeys(
        @Nonnull ConnectionHandle ch,
        String catalog,
        String schema,
        String table
    ) {
        return getConnectionMeta(ch).getPrimaryKeys(ch, catalog, schema, table);
    }

    @Override
    public MetaResultSet getImportedKeys(
        @Nonnull ConnectionHandle ch,
        String catalog,
        String schema,
        String table
    ) {
        return getConnectionMeta(ch).getImportedKeys(ch, catalog, schema, table);
    }

    @Override
    public MetaResultSet getExportedKeys(
        @Nonnull ConnectionHandle ch,
        String catalog,
        String schema,
        String table
    ) {
        return getConnectionMeta(ch).getExportedKeys(ch, catalog, schema, table);
    }

    @Override
    public MetaResultSet getCrossReference(
        @Nonnull ConnectionHandle ch,
        String parentCatalog,
        String parentSchema,
        String parentTable,
        String foreignCatalog,
        String foreignSchema,
        String foreignTable
    ) {
        return getConnectionMeta(ch).getCrossReference(
            ch,
            parentCatalog,
            parentSchema,
            parentTable,
            foreignCatalog,
            foreignSchema,
            foreignTable
        );
    }

    @Override
    public MetaResultSet getTypeInfo(@Nonnull ConnectionHandle ch) {
        return getConnectionMeta(ch).getTypeInfo(ch);
    }

    @Override
    public MetaResultSet getIndexInfo(
        @Nonnull ConnectionHandle ch,
        String catalog,
        String schema,
        String table,
        boolean unique,
        boolean approximate
    ) {
        return getConnectionMeta(ch).getIndexInfo(ch, catalog, schema, table, unique, approximate);
    }

    @Override
    public MetaResultSet getUDTs(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat typeNamePattern,
        int[] types
    ) {
        return getConnectionMeta(ch).getUDTs(ch, catalog, schemaPattern, typeNamePattern, types);
    }

    @Override
    public MetaResultSet getSuperTypes(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat typeNamePattern
    ) {
        return getConnectionMeta(ch).getSuperTypes(ch, catalog, schemaPattern, typeNamePattern);
    }

    @Override
    public MetaResultSet getSuperTables(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern
    ) {
        return getConnectionMeta(ch).getSuperTables(ch, catalog, schemaPattern, tableNamePattern);
    }

    @Override
    public MetaResultSet getAttributes(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat typeNamePattern,
        Pat attributeNamePattern
    ) {
        return getConnectionMeta(ch).getAttributes(ch, catalog, schemaPattern, typeNamePattern,
            attributeNamePattern);
    }

    @Override
    public MetaResultSet getClientInfoProperties(@Nonnull ConnectionHandle ch) {
        return getConnectionMeta(ch).getClientInfoProperties(ch);
    }

    @Override
    public MetaResultSet getFunctions(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat functionNamePattern
    ) {
        return getConnectionMeta(ch).getFunctions(ch, catalog, schemaPattern, functionNamePattern);
    }

    @Override
    public MetaResultSet getFunctionColumns(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat functionNamePattern,
        Pat columnNamePattern
    ) {
        return getConnectionMeta(ch).getFunctionColumns(
            ch,
            catalog,
            schemaPattern,
            functionNamePattern,
            columnNamePattern
        );
    }

    @Override
    public MetaResultSet getPseudoColumns(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern,
        Pat columnNamePattern
    ) {
        return getConnectionMeta(ch).getPseudoColumns(
            ch,
            catalog,
            schemaPattern,
            tableNamePattern,
            columnNamePattern
        );
    }

    @Override
    public Iterable<Object> createIterable(
        @Nonnull StatementHandle sh,
        QueryState state,
        Signature signature,
        List<TypedValue> parameters,
        Frame firstFrame
    ) {
        DingoConnection connection = connectionMap.get(sh.connectionId);
        StatementHandle newSh = new StatementHandle(connection.id, sh.id, sh.signature);
        return connection.getMeta().createIterable(
            newSh,
            state,
            signature,
            parameters,
            firstFrame
        );
    }

    @Override
    public StatementHandle prepare(@Nonnull ConnectionHandle ch, String sql, long maxRowCount) {
        if (log.isDebugEnabled()) {
            log.debug("connection handle = {}, sql = {}, maxRowCount = {}.", ch, sql, maxRowCount);
        }
        DingoConnection connection = connectionMap.get(ch.id);
        try {
            DingoPreparedStatement prepareStatement = (DingoPreparedStatement) connection.prepareStatement(sql);
            StatementHandle handle = prepareStatement.handle;
            return new StatementHandle(ch.id, handle.id, handle.signature);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Deprecated
    @Override
    public ExecuteResult prepareAndExecute(
        @Nonnull StatementHandle sh,
        String sql,
        long maxRowCount,
        PrepareCallback callback
    ) {
        return null;
    }

    @Override
    public ExecuteResult prepareAndExecute(
        @Nonnull StatementHandle sh,
        String sql,
        long maxRowCount,
        int maxRowsInFirstFrame,
        PrepareCallback callback // This callback does nothing
    ) throws NoSuchStatementException {
        if (log.isDebugEnabled()) {
            log.debug("statement handle = {}, sql = {}, maxRowCount = {}, maxRowsInFirstFrame = {}.",
                sh, sql, maxRowCount, maxRowsInFirstFrame);
        }
        DingoConnection connection = connectionMap.get(sh.connectionId);
        StatementHandle newSh = new StatementHandle(connection.id, sh.id, sh.signature);
        DingoStatement statement = (DingoStatement) connection.getStatement(newSh);
        return connection.getMeta().prepareAndExecute(
            newSh,
            sql,
            maxRowCount,
            maxRowsInFirstFrame,
            new PrepareCallback() {
                @Override
                public Object getMonitor() {
                    return statement;
                }

                @Override
                public void clear() throws SQLException {
                    statement.clear();
                }

                @Override
                public void assign(Signature signature, Frame firstFrame, long updateCount) throws SQLException {
                    statement.assign(signature, firstFrame, updateCount, sql);
                }

                @Override
                public void execute() {
                }
            }
        );
    }

    @Override
    public ExecuteBatchResult prepareAndExecuteBatch(
        @Nonnull StatementHandle sh,
        List<String> sqlCommands
    ) throws NoSuchStatementException {
        DingoConnection connection = connectionMap.get(sh.connectionId);
        StatementHandle newSh = new StatementHandle(connection.id, sh.id, sh.signature);
        return connection.getMeta().prepareAndExecuteBatch(newSh, sqlCommands);
    }

    @Override
    public ExecuteBatchResult executeBatch(
        @Nonnull StatementHandle sh,
        List<List<TypedValue>> parameterValues
    ) throws NoSuchStatementException {
        DingoConnection connection = connectionMap.get(sh.connectionId);
        StatementHandle newSh = new StatementHandle(connection.id, sh.id, sh.signature);
        return connection.getMeta().executeBatch(newSh, parameterValues);
    }

    @Override
    public Frame fetch(
        @Nonnull StatementHandle sh,
        long offset,
        int fetchMaxRowCount
    ) throws NoSuchStatementException, MissingResultsException {
        if (log.isDebugEnabled()) {
            log.debug("statement handle = {}, offset = {}, fetchMaxRowCount = {}.", sh, offset, fetchMaxRowCount);
        }
        DingoConnection connection = connectionMap.get(sh.connectionId);
        StatementHandle newSh = new StatementHandle(connection.id, sh.id, sh.signature);
        return connection.getMeta().fetch(newSh, offset, fetchMaxRowCount);
    }

    @Deprecated
    @Override
    public ExecuteResult execute(
        @Nonnull StatementHandle sh,
        List<TypedValue> parameterValues,
        long maxRowCount
    ) {
        return null;
    }

    @Override
    public ExecuteResult execute(
        @Nonnull StatementHandle sh,
        List<TypedValue> parameterValues,
        int maxRowsInFirstFrame
    ) throws NoSuchStatementException {
        if (log.isDebugEnabled()) {
            log.debug("statement handle = {}, parameterValues = {}, maxRowInFirstFrame = {}.",
                sh,
                TypedValue.values(parameterValues).stream()
                    .map(Objects::toString)
                    .collect(Collectors.joining(", ")),
                maxRowsInFirstFrame
            );
        }
        DingoConnection connection = connectionMap.get(sh.connectionId);
        StatementHandle newSh = new StatementHandle(connection.id, sh.id, sh.signature);
        try {
            DingoPreparedStatement statement = (DingoPreparedStatement) connection.getStatement(newSh);
            synchronized (connection.getStatement(newSh)) {
                statement.clear();
                int updateCount;
                switch (sh.signature.statementType) {
                    case CREATE:
                    case DROP:
                    case ALTER:
                    case OTHER_DDL:
                        updateCount = 0; // DDL produces no result set
                        break;
                    default:
                        updateCount = -1; // SELECT and DML produces result set
                        break;
                }
                statement.assign(sh.signature, null, updateCount, sh.signature.sql);
                statement.setParameterValues(parameterValues);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // Instead of call execute of `DingoMeta`, construct `ExecuteResult` here to use our own connection id.
        return DingoMeta.createExecuteResult(sh);
    }

    @Override
    public StatementHandle createStatement(@Nonnull ConnectionHandle ch) {
        if (log.isDebugEnabled()) {
            log.debug("ch = {}.", ch);
        }
        DingoConnection connection = connectionMap.get(ch.id);
        try {
            AvaticaStatement statement = connection.createStatement();
            if (log.isDebugEnabled()) {
                log.debug("Statement created, handle = {}.", statement.handle);
            }
            StatementHandle handle = statement.handle;
            return new StatementHandle(ch.id, handle.id, handle.signature);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeStatement(@Nonnull StatementHandle sh) {
        if (log.isDebugEnabled()) {
            log.debug("statement handle = {}.", sh);
        }
        DingoConnection connection = connectionMap.get(sh.connectionId);
        StatementHandle newSh = new StatementHandle(connection.id, sh.id, sh.signature);
        try {
            AvaticaStatement statement = connection.getStatement(newSh);
            statement.close();
            return;
        } catch (NoSuchStatementException | SQLException e) {
            log.error("Failed to close statement: handle = {}.", sh, e);
        }
        log.warn("The connection (handle = {}) is not found.", sh.connectionId);
    }

    // Here the local meta is created.
    @Override
    public void openConnection(@Nonnull ConnectionHandle ch, Map<String, String> info) {
        if (log.isDebugEnabled()) {
            log.debug("connection handle = {}, info = {}.", ch, info);
        }
        Properties properties = new Properties();
        properties.putAll(info);
        DingoConnection connection = DingoDriver.INSTANCE.createConnection(null, properties);
        connectionMap.put(ch.id, connection);
    }

    @Override
    public void closeConnection(@Nonnull ConnectionHandle ch) {
        if (log.isDebugEnabled()) {
            log.debug("connection handle = {}.", ch);
        }
        DingoConnection connection = connectionMap.remove(ch.id);
        if (connection != null) {
            try {
                connection.close();
                return;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        log.warn("The connection (handle = {}) is not found.", ch);
    }

    @Override
    public boolean syncResults(
        @Nonnull StatementHandle sh,
        QueryState state,
        long offset
    ) throws NoSuchStatementException {
        DingoConnection connection = connectionMap.get(sh.connectionId);
        StatementHandle newSh = new StatementHandle(connection.id, sh.id, sh.signature);
        return connection.getMeta().syncResults(newSh, state, offset);
    }

    @Override
    public void commit(@Nonnull ConnectionHandle ch) {
        getConnectionMeta(ch).commit(ch);
    }

    @Override
    public void rollback(@Nonnull ConnectionHandle ch) {
        getConnectionMeta(ch).rollback(ch);
    }

    @Override
    public ConnectionProperties connectionSync(@Nonnull ConnectionHandle ch, ConnectionProperties connProps) {
        return getConnectionMeta(ch).connectionSync(ch, connProps);
    }
}
