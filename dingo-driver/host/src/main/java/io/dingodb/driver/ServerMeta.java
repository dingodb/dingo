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
import io.dingodb.common.auth.Authentication;
import io.dingodb.common.config.SecurityConfiguration;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionStatus;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.verify.auth.IdentityAuthService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

// Only one meta instance exists in avatica server.
// On each request, create a local meta (which is light-weighted) to do the work.
@Slf4j
public class ServerMeta implements Meta {
    public final Map<String, DingoConnection> connectionMap = new ConcurrentHashMap<>();

    private static final ServerMeta INSTANCE = new ServerMeta();

    public static ServerMeta getInstance() {
        return INSTANCE;
    }

    private static @NonNull MetaResultSet mapMetaResultSet(String connectionId, @NonNull MetaResultSet resultSet) {
        if (resultSet.signature != null) {
            // It is a query result set.
            return MetaResultSet.create(
                connectionId,
                resultSet.statementId,
                resultSet.ownStatement,
                resultSet.signature,
                resultSet.firstFrame,
                resultSet.updateCount
            );
        }
        // It is a DML result set.
        return MetaResultSet.count(connectionId, resultSet.statementId, resultSet.updateCount);
    }

    private @NonNull Meta getConnectionMeta(@NonNull ConnectionHandle ch) {
        DingoConnection connection = connectionMap.get(ch.id);
        return connection.getMeta();
    }

    @Override
    public Map<DatabaseProperty, Object> getDatabaseProperties(@NonNull ConnectionHandle ch) {
        return getConnectionMeta(ch).getDatabaseProperties(ch);
    }

    @Override
    public MetaResultSet getTables(
        @NonNull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern,
        List<String> typeList
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getTables(ch, catalog, schemaPattern, tableNamePattern, typeList)
        );
    }

    @Override
    public MetaResultSet getColumns(
        @NonNull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern,
        Pat columnNamePattern
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getColumns(ch, catalog, schemaPattern, tableNamePattern, columnNamePattern)
        );
    }

    @Override
    public MetaResultSet getSchemas(@NonNull ConnectionHandle ch, String catalog, Pat schemaPattern) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getSchemas(ch, catalog, schemaPattern)
        );
    }

    @Override
    public MetaResultSet getCatalogs(@NonNull ConnectionHandle ch) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getCatalogs(ch)
        );
    }

    @Override
    public MetaResultSet getTableTypes(@NonNull ConnectionHandle ch) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getTableTypes(ch)
        );
    }

    @Override
    public MetaResultSet getProcedures(
        @NonNull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat procedureNamePattern
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getProcedures(ch, catalog, schemaPattern, procedureNamePattern)
        );
    }

    @Override
    public MetaResultSet getProcedureColumns(
        @NonNull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat procedureNamePattern,
        Pat columnNamePattern
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getProcedureColumns(
                ch,
                catalog,
                schemaPattern,
                procedureNamePattern,
                columnNamePattern
            )
        );
    }

    @Override
    public MetaResultSet getColumnPrivileges(
        @NonNull ConnectionHandle ch,
        String catalog,
        String schema,
        String table,
        Pat columnNamePattern
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getColumnPrivileges(ch, catalog, schema, table, columnNamePattern)
        );
    }

    @Override
    public MetaResultSet getTablePrivileges(
        @NonNull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getTablePrivileges(ch, catalog, schemaPattern, tableNamePattern)
        );
    }

    @Override
    public MetaResultSet getBestRowIdentifier(
        @NonNull ConnectionHandle ch,
        String catalog,
        String schema,
        String table,
        int scope,
        boolean nullable
    ) {
        if (log.isDebugEnabled()) {
            log.debug("connection handle = {}, catalog = {}, schema = {}, table = {}, scope = {}, nullable = {}",
                ch, catalog, schema, table, scope, nullable);
        }
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getBestRowIdentifier(ch, catalog, schema, table, scope, nullable)
        );
    }

    @Override
    public MetaResultSet getVersionColumns(
        @NonNull ConnectionHandle ch,
        String catalog,
        String schema,
        String table
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getVersionColumns(ch, catalog, schema, table)
        );
    }

    @Override
    public MetaResultSet getPrimaryKeys(
        @NonNull ConnectionHandle ch,
        String catalog,
        String schema,
        String table
    ) {
        if (log.isDebugEnabled()) {
            log.debug("connection handle = {}, catalog = {}, schema = {}, table = {}.", ch, catalog, schema, table);
        }
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getPrimaryKeys(ch, catalog, schema, table)
        );
    }

    @Override
    public MetaResultSet getImportedKeys(
        @NonNull ConnectionHandle ch,
        String catalog,
        String schema,
        String table
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getImportedKeys(ch, catalog, schema, table)
        );
    }

    @Override
    public MetaResultSet getExportedKeys(
        @NonNull ConnectionHandle ch,
        String catalog,
        String schema,
        String table
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getExportedKeys(ch, catalog, schema, table)
        );
    }

    @Override
    public MetaResultSet getCrossReference(
        @NonNull ConnectionHandle ch,
        String parentCatalog,
        String parentSchema,
        String parentTable,
        String foreignCatalog,
        String foreignSchema,
        String foreignTable
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getCrossReference(
                ch,
                parentCatalog,
                parentSchema,
                parentTable,
                foreignCatalog,
                foreignSchema,
                foreignTable
            )
        );
    }

    @Override
    public MetaResultSet getTypeInfo(@NonNull ConnectionHandle ch) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getTypeInfo(ch)
        );
    }

    @Override
    public MetaResultSet getIndexInfo(
        @NonNull ConnectionHandle ch,
        String catalog,
        String schema,
        String table,
        boolean unique,
        boolean approximate
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getIndexInfo(ch, catalog, schema, table, unique, approximate)
        );
    }

    @Override
    public MetaResultSet getUDTs(
        @NonNull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat typeNamePattern,
        int[] types
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getUDTs(ch, catalog, schemaPattern, typeNamePattern, types)
        );
    }

    @Override
    public MetaResultSet getSuperTypes(
        @NonNull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat typeNamePattern
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getSuperTypes(ch, catalog, schemaPattern, typeNamePattern)
        );
    }

    @Override
    public MetaResultSet getSuperTables(
        @NonNull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getSuperTables(ch, catalog, schemaPattern, tableNamePattern)
        );
    }

    @Override
    public MetaResultSet getAttributes(
        @NonNull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat typeNamePattern,
        Pat attributeNamePattern
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getAttributes(
                ch,
                catalog,
                schemaPattern,
                typeNamePattern,
                attributeNamePattern
            )
        );
    }

    @Override
    public MetaResultSet getClientInfoProperties(@NonNull ConnectionHandle ch) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getClientInfoProperties(ch)
        );
    }

    @Override
    public MetaResultSet getFunctions(
        @NonNull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat functionNamePattern
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getFunctions(ch, catalog, schemaPattern, functionNamePattern)
        );
    }

    @Override
    public MetaResultSet getFunctionColumns(
        @NonNull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat functionNamePattern,
        Pat columnNamePattern
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getFunctionColumns(
                ch,
                catalog,
                schemaPattern,
                functionNamePattern,
                columnNamePattern
            )
        );
    }

    @Override
    public MetaResultSet getPseudoColumns(
        @NonNull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern,
        Pat columnNamePattern
    ) {
        return mapMetaResultSet(
            ch.id,
            getConnectionMeta(ch).getPseudoColumns(
                ch,
                catalog,
                schemaPattern,
                tableNamePattern,
                columnNamePattern
            )
        );
    }

    // This is never called at server side.
    @Override
    public Iterable<Object> createIterable(
        @NonNull StatementHandle sh,
        QueryState state,
        Signature signature,
        List<TypedValue> parameters,
        Frame firstFrame
    ) {
        return null;
    }

    @Override
    @SneakyThrows
    public StatementHandle prepare(@NonNull ConnectionHandle ch, String sql, long maxRowCount) {
        if (log.isDebugEnabled()) {
            log.debug("connection handle = {}, sql = {}, maxRowCount = {}.", ch, sql, maxRowCount);
        }
        DingoConnection connection = connectionMap.get(ch.id);
        DingoPreparedStatement prepareStatement = (DingoPreparedStatement) connection.prepareStatement(sql);
        StatementHandle handle = prepareStatement.handle;
        prepareStatement.setSignature(handle.signature);
        return new StatementHandle(ch.id, handle.id, handle.signature);
    }

    @Deprecated
    @Override
    public ExecuteResult prepareAndExecute(
        @NonNull StatementHandle sh,
        String sql,
        long maxRowCount,
        PrepareCallback callback
    ) {
        return null;
    }

    @Override
    public ExecuteResult prepareAndExecute(
        @NonNull StatementHandle sh,
        String sql,
        long maxRowCount,
        int maxRowsInFirstFrame,
        PrepareCallback callback // This callback does nothing
    ) throws NoSuchStatementException {
        if (log.isDebugEnabled()) {
            log.debug("statement handle = {}, sql = {}, maxRowCount = {}, maxRowsInFirstFrame = {}.",
                sh, sql, maxRowCount, maxRowsInFirstFrame);
        }
        final String connectionId = sh.connectionId;
        DingoConnection connection = connectionMap.get(connectionId);
        StatementHandle newSh = new StatementHandle(connection.id, sh.id, sh.signature);
        DingoStatement statement = (DingoStatement) connection.getStatement(newSh);
        ExecuteResult executeResult = connection.getMeta().prepareAndExecute(
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
                public void clear() {
                }

                @Override
                public void assign(Signature signature, Frame firstFrame, long updateCount) throws SQLException {
                    statement.setSignature(signature);
                    if (updateCount == -1) {
                        statement.createResultSet(firstFrame);
                    }
                }

                @Override
                public void execute() {
                }
            }
        );
        return new ExecuteResult(
            executeResult.resultSets.stream()
                .map(r -> mapMetaResultSet(connectionId, r))
                .collect(Collectors.toList())
        );
    }

    @Override
    public ExecuteBatchResult prepareAndExecuteBatch(
        @NonNull StatementHandle sh,
        List<String> sqlCommands
    ) throws NoSuchStatementException {
        DingoConnection connection = connectionMap.get(sh.connectionId);
        StatementHandle newSh = new StatementHandle(connection.id, sh.id, sh.signature);
        return connection.getMeta().prepareAndExecuteBatch(newSh, sqlCommands);
    }

    @Override
    public ExecuteBatchResult executeBatch(
        @NonNull StatementHandle sh,
        List<List<TypedValue>> parameterValues
    ) throws NoSuchStatementException {
        DingoConnection connection = connectionMap.get(sh.connectionId);
        StatementHandle newSh = new StatementHandle(connection.id, sh.id, sh.signature);
        return connection.getMeta().executeBatch(newSh, parameterValues);
    }

    @Override
    public Frame fetch(
        @NonNull StatementHandle sh,
        long offset,
        int fetchMaxRowCount
    ) throws NoSuchStatementException {
        if (log.isDebugEnabled()) {
            log.debug("statement handle = {}, offset = {}, fetchMaxRowCount = {}.", sh, offset, fetchMaxRowCount);
        }
        DingoConnection connection = connectionMap.get(sh.connectionId);
        StatementHandle newSh = new StatementHandle(connection.id, sh.id, sh.signature);
        Frame frame = connection.getMeta().fetch(newSh, offset, fetchMaxRowCount);
        connection.setCommandStartTime(0);
        return frame;
    }

    @Deprecated
    @Override
    public ExecuteResult execute(
        @NonNull StatementHandle sh,
        List<TypedValue> parameterValues,
        long maxRowCount
    ) {
        return null;
    }

    @Override
    public ExecuteResult execute(
        @NonNull StatementHandle sh,
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
        final String connectionId = sh.connectionId;
        DingoConnection connection = connectionMap.get(connectionId);
        // `sh.signature` may be `null` for the client is trying to save ser-des cost.
        StatementHandle newSh = new StatementHandle(connection.id, sh.id, sh.signature);
        try {
            DingoPreparedStatement statement = (DingoPreparedStatement) connection.getStatement(newSh);
            MetaResultSet resultSet;
            synchronized (connection.getStatement(newSh)) {
                statement.setParameterValues(parameterValues);
                ExecuteResult executeResult = connection.getMeta().execute(
                    newSh,
                    parameterValues,
                    maxRowsInFirstFrame
                );
                // There are always 1 resultSet.
                resultSet = executeResult.resultSets.get(0);
                if (resultSet.updateCount == -1) {
                    statement.createResultSet(resultSet.firstFrame);
                }
            }
            return new ExecuteResult(ImmutableList.of(mapMetaResultSet(connectionId, resultSet)));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StatementHandle createStatement(@NonNull ConnectionHandle ch) {
        if (log.isDebugEnabled()) {
            log.debug("connection handle = {}.", ch);
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
    public void closeStatement(@NonNull StatementHandle sh) {
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

    public void cancelStatement(@NonNull String connectionId, int id, Meta.Signature signature) {
        DingoConnection connection = connectionMap.get(connectionId);
        StatementHandle newSh = new StatementHandle(connection.id, id, signature);
        try {
            log.info("statement handle = {}.", newSh);
            AvaticaStatement statement = connection.getStatement(newSh);
            statement.cancel();
            ITransaction transaction = connection.getTransaction();
            if (transaction != null) {
                if (transaction.isAutoCommit()) {
                    if (transaction.getStatus() == TransactionStatus.START) {
                        log.info("cancelStatement, {} rollback ...", transaction.getTxnId());
                        connection.rollback();
                    } else {
                        log.info("cancelStatement, cancel transaction {} ", transaction.getTxnId());
                        transaction.cancel();
                    }
                }
            }
            return;
        } catch (NoSuchStatementException | SQLException e) {
            log.error("Failed to cancel statement: handle = {}.", newSh, e);
        }
        log.warn("The connection (handle = {}) is not found.", connectionId);
    }


    // Here the local meta is created.
    @Override
    public void openConnection(@NonNull ConnectionHandle ch, Map<String, String> info) {
        if (log.isDebugEnabled()) {
            log.debug("connection handle = {}, info = {}.", ch, info);
        }
        Properties properties = new Properties();
        properties.putAll(info);
        DingoConnection connection = DingoDriver.INSTANCE.createConnection(null, properties);
        connectionMap.put(ch.id, connection);
        loadGlobalVariables(connection);
        // connection with init db
        connectionUrlSync(ch, properties);
        if (SecurityConfiguration.isAuth()) {
            Authentication authentication = Authentication.builder()
                .username(info.get("user"))
                .host(info.get("host"))
                .password(info.get("password"))
                .build();
            try {
                IdentityAuthService.INSTANCE.validate(authentication);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized static void loadGlobalVariables(DingoConnection dingoConnection) {
        if (ScopeVariables.globalVariables.size() > 0) {
            dingoConnection.setClientInfo(ScopeVariables.globalVariables);
            return;
        }
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        Map<String, String> globalVariableMap = infoSchemaService.getGlobalVariables();
        ScopeVariables.globalVariables.putAll(globalVariableMap);
        dingoConnection.setClientInfo(ScopeVariables.globalVariables);
    }

    @Override
    public void closeConnection(@NonNull ConnectionHandle ch) {
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
        @NonNull StatementHandle sh,
        QueryState state,
        long offset
    ) throws NoSuchStatementException {
        DingoConnection connection = connectionMap.get(sh.connectionId);
        StatementHandle newSh = new StatementHandle(connection.id, sh.id, sh.signature);
        return connection.getMeta().syncResults(newSh, state, offset);
    }

    @Override
    public void commit(@NonNull ConnectionHandle ch) {
        getConnectionMeta(ch).commit(ch);
    }

    @Override
    public void rollback(@NonNull ConnectionHandle ch) {
        getConnectionMeta(ch).rollback(ch);
    }

    @Override
    public ConnectionProperties connectionSync(@NonNull ConnectionHandle ch, ConnectionProperties connProps) {
        return getConnectionMeta(ch).connectionSync(ch, connProps);
    }

    /**
     * use schema.
     * @param ch ch
     * @param info db
     */
    public void connectionUrlSync(ConnectionHandle ch, Properties info) {
        if (info.containsKey("dbname")) {
            String schemaName = (String) info.get("dbname");
            connectionSync(
                ch,
                new ConnectionPropertiesImpl(null, null, null, null, schemaName)
            );
        }
    }
}
