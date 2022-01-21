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

import io.dingodb.driver.DingoDriver;
import io.dingodb.driver.DingoFactory;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nonnull;

// Only one meta instance exists in avatica server.
// Bridge it to connection specified meta of local driver.
public class ServerMeta implements Meta {
    private final Map<String, Meta> metaMap = new LinkedHashMap<>();

    public ServerMeta() {
    }

    @Override
    public Map<DatabaseProperty, Object> getDatabaseProperties(@Nonnull ConnectionHandle ch) {
        Meta meta = metaMap.get(ch.id);
        return meta.getDatabaseProperties(ch);
    }

    @Override
    public MetaResultSet getTables(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern,
        List<String> typeList
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getTables(ch, catalog, schemaPattern, tableNamePattern, typeList);
    }

    @Override
    public MetaResultSet getColumns(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern,
        Pat columnNamePattern
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getColumns(ch, catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    @Override
    public MetaResultSet getSchemas(@Nonnull ConnectionHandle ch, String catalog, Pat schemaPattern) {
        Meta meta = metaMap.get(ch.id);
        return meta.getSchemas(ch, catalog, schemaPattern);
    }

    @Override
    public MetaResultSet getCatalogs(@Nonnull ConnectionHandle ch) {
        Meta meta = metaMap.get(ch.id);
        return meta.getCatalogs(ch);
    }

    @Override
    public MetaResultSet getTableTypes(@Nonnull ConnectionHandle ch) {
        Meta meta = metaMap.get(ch.id);
        return meta.getTableTypes(ch);
    }

    @Override
    public MetaResultSet getProcedures(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat procedureNamePattern
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getProcedures(ch, catalog, schemaPattern, procedureNamePattern);
    }

    @Override
    public MetaResultSet getProcedureColumns(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat procedureNamePattern,
        Pat columnNamePattern
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getProcedureColumns(ch, catalog, schemaPattern, procedureNamePattern, columnNamePattern);
    }

    @Override
    public MetaResultSet getColumnPrivileges(
        @Nonnull ConnectionHandle ch,
        String catalog,
        String schema,
        String table,
        Pat columnNamePattern
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getColumnPrivileges(ch, catalog, schema, table, columnNamePattern);
    }

    @Override
    public MetaResultSet getTablePrivileges(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getTablePrivileges(ch, catalog, schemaPattern, tableNamePattern);
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
        Meta meta = metaMap.get(ch.id);
        return meta.getBestRowIdentifier(ch, catalog, schema, table, scope, nullable);
    }

    @Override
    public MetaResultSet getVersionColumns(
        @Nonnull ConnectionHandle ch,
        String catalog,
        String schema,
        String table
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getVersionColumns(ch, catalog, schema, table);
    }

    @Override
    public MetaResultSet getPrimaryKeys(
        @Nonnull ConnectionHandle ch,
        String catalog,
        String schema,
        String table
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getPrimaryKeys(ch, catalog, schema, table);
    }

    @Override
    public MetaResultSet getImportedKeys(
        @Nonnull ConnectionHandle ch,
        String catalog,
        String schema,
        String table
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getImportedKeys(ch, catalog, schema, table);
    }

    @Override
    public MetaResultSet getExportedKeys(
        @Nonnull ConnectionHandle ch,
        String catalog,
        String schema,
        String table
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getExportedKeys(ch, catalog, schema, table);
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
        Meta meta = metaMap.get(ch.id);
        return meta.getCrossReference(
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
        Meta meta = metaMap.get(ch.id);
        return meta.getTypeInfo(ch);
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
        Meta meta = metaMap.get(ch.id);
        return meta.getIndexInfo(ch, catalog, schema, table, unique, approximate);
    }

    @Override
    public MetaResultSet getUDTs(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat typeNamePattern,
        int[] types
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getUDTs(ch, catalog, schemaPattern, typeNamePattern, types);
    }

    @Override
    public MetaResultSet getSuperTypes(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat typeNamePattern
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getSuperTypes(ch, catalog, schemaPattern, typeNamePattern);
    }

    @Override
    public MetaResultSet getSuperTables(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getSuperTables(ch, catalog, schemaPattern, tableNamePattern);
    }

    @Override
    public MetaResultSet getAttributes(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat typeNamePattern,
        Pat attributeNamePattern
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getAttributes(ch, catalog, schemaPattern, typeNamePattern, attributeNamePattern);
    }

    @Override
    public MetaResultSet getClientInfoProperties(@Nonnull ConnectionHandle ch) {
        Meta meta = metaMap.get(ch.id);
        return meta.getClientInfoProperties(ch);
    }

    @Override
    public MetaResultSet getFunctions(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat functionNamePattern
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getFunctions(ch, catalog, schemaPattern, functionNamePattern);
    }

    @Override
    public MetaResultSet getFunctionColumns(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat functionNamePattern,
        Pat columnNamePattern
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getFunctionColumns(ch, catalog, schemaPattern, functionNamePattern, columnNamePattern);
    }

    @Override
    public MetaResultSet getPseudoColumns(
        @Nonnull ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern,
        Pat columnNamePattern
    ) {
        Meta meta = metaMap.get(ch.id);
        return meta.getPseudoColumns(ch, catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    @Override
    public Iterable<Object> createIterable(
        @Nonnull StatementHandle sh,
        QueryState state,
        Signature signature,
        List<TypedValue> parameters,
        Frame firstFrame
    ) {
        Meta meta = metaMap.get(sh.connectionId);
        return meta.createIterable(sh, state, signature, parameters, firstFrame);
    }

    @Override
    public StatementHandle prepare(@Nonnull ConnectionHandle ch, String sql, long maxRowCount) {
        Meta meta = metaMap.get(ch.id);
        return meta.prepare(ch, sql, maxRowCount);
    }

    @Deprecated
    @Override
    public ExecuteResult prepareAndExecute(
        @Nonnull StatementHandle sh,
        String sql,
        long maxRowCount,
        PrepareCallback callback
    ) throws NoSuchStatementException {
        Meta meta = metaMap.get(sh.connectionId);
        return meta.prepareAndExecute(sh, sql, maxRowCount, callback);
    }

    @Override
    public ExecuteResult prepareAndExecute(
        @Nonnull StatementHandle sh,
        String sql,
        long maxRowCount,
        int maxRowsInFirstFrame,
        PrepareCallback callback // This callback does nothing
    ) throws NoSuchStatementException {
        Meta meta = metaMap.get(sh.connectionId);
        return meta.prepareAndExecute(
            sh,
            sql,
            maxRowCount,
            maxRowsInFirstFrame,
            new PrepareCallback() {
                @Override
                public Object getMonitor() {
                    return callback.getMonitor();
                }

                @Override
                public void clear() throws SQLException {
                    callback.clear();
                }

                @Override
                public void assign(Signature signature, Frame firstFrame, long updateCount) throws SQLException {
                    callback.assign(signature, firstFrame, updateCount);
                }

                @Override
                public void execute() throws SQLException {
                    callback.execute();
                }
            }
        );
    }

    @Override
    public ExecuteBatchResult prepareAndExecuteBatch(
        @Nonnull StatementHandle sh,
        List<String> sqlCommands
    ) throws NoSuchStatementException {
        Meta meta = metaMap.get(sh.connectionId);
        return meta.prepareAndExecuteBatch(sh, sqlCommands);
    }

    @Override
    public ExecuteBatchResult executeBatch(
        @Nonnull StatementHandle sh,
        List<List<TypedValue>> parameterValues
    ) throws NoSuchStatementException {
        Meta meta = metaMap.get(sh.connectionId);
        return meta.executeBatch(sh, parameterValues);
    }

    @Override
    public Frame fetch(
        @Nonnull StatementHandle sh,
        long offset,
        int fetchMaxRowCount
    ) throws NoSuchStatementException, MissingResultsException {
        Meta meta = metaMap.get(sh.connectionId);
        return meta.fetch(sh, offset, fetchMaxRowCount);
    }

    @Deprecated
    @Override
    public ExecuteResult execute(
        @Nonnull StatementHandle sh,
        List<TypedValue> parameterValues,
        long maxRowCount
    ) throws NoSuchStatementException {
        Meta meta = metaMap.get(sh.connectionId);
        return meta.execute(sh, parameterValues, maxRowCount);
    }

    @Override
    public ExecuteResult execute(
        @Nonnull StatementHandle sh,
        List<TypedValue> parameterValues,
        int maxRowsInFirstFrame
    ) throws NoSuchStatementException {
        Meta meta = metaMap.get(sh.connectionId);
        return meta.execute(sh, parameterValues, maxRowsInFirstFrame);
    }

    @Override
    public StatementHandle createStatement(@Nonnull ConnectionHandle ch) {
        Meta meta = metaMap.get(ch.id);
        return meta.createStatement(ch);
    }

    @Override
    public void closeStatement(@Nonnull StatementHandle sh) {
        Meta meta = metaMap.get(sh.connectionId);
        meta.closeStatement(sh);
    }

    // Here the local meta is created.
    @Override
    public void openConnection(@Nonnull ConnectionHandle ch, Map<String, String> info) {
        Properties properties = new Properties();
        properties.putAll(info);
        metaMap.computeIfAbsent(ch.id, (id) -> DingoDriver.INSTANCE.createMeta(
            DingoFactory.INSTANCE.newConnection(
                DingoDriver.INSTANCE,
                DingoFactory.INSTANCE,
                null,
                properties
            )
        ));
    }

    @Override
    public void closeConnection(@Nonnull ConnectionHandle ch) {
        Meta meta = metaMap.get(ch.id);
        meta.closeConnection(ch);
        metaMap.remove(ch.id);
    }

    @Override
    public boolean syncResults(
        @Nonnull StatementHandle sh,
        QueryState state,
        long offset
    ) throws NoSuchStatementException {
        Meta meta = metaMap.get(sh.connectionId);
        return meta.syncResults(sh, state, offset);
    }

    @Override
    public void commit(@Nonnull ConnectionHandle ch) {
        Meta meta = metaMap.get(ch.id);
        meta.commit(ch);
    }

    @Override
    public void rollback(@Nonnull ConnectionHandle ch) {
        Meta meta = metaMap.get(ch.id);
        meta.rollback(ch);
    }

    @Override
    public ConnectionProperties connectionSync(@Nonnull ConnectionHandle ch, ConnectionProperties connProps) {
        Meta meta = metaMap.get(ch.id);
        return meta.connectionSync(ch, connProps);
    }
}
