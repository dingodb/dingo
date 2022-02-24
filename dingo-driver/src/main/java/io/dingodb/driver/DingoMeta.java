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
import com.google.common.collect.ImmutableMap;
import io.dingodb.calcite.DingoSchema;
import io.dingodb.exec.Services;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.sql.parser.SqlParseException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

@Slf4j
class DingoMeta extends MetaImpl {
    public DingoMeta(DingoConnection connection) {
        super(connection);
    }

    @Override
    public StatementHandle prepare(
        ConnectionHandle ch,
        String sql,
        long maxRowCount
    ) {
        return null;
    }

    @Override
    public ExecuteResult prepareAndExecute(
        StatementHandle sh,
        String sql,
        long maxRowCount,
        PrepareCallback callback
    ) throws NoSuchStatementException {
        return null;
    }

    @Override
    public ExecuteResult prepareAndExecute(
        @Nonnull StatementHandle sh,
        String sql,
        long maxRowCount,
        int maxRowsInFirstFrame,
        @Nonnull PrepareCallback callback
    ) throws NoSuchStatementException {
        try {
            DingoConnection dingoConnection = (DingoConnection) connection;
            DingoConnection.DingoContext context = dingoConnection.createContext();
            DingoDriverParser parser = new DingoDriverParser(context.getParserContext());
            final DingoSignature signature = parser.parseQuery(sql, context);
            sh.signature = signature;
            final int updateCount;
            switch (signature.statementType) {
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
            synchronized (callback.getMonitor()) {
                callback.clear();
                // For local driver, here signature is assigned to statement.
                // Buf not for remote driver. Don't know why.
                callback.assign(signature, null, updateCount);
            }
            // For local driver, here `fetch` is called.
            callback.execute();
            final MetaResultSet metaResultSet = MetaResultSet.create(
                sh.connectionId,
                sh.id,
                false,
                signature,
                null,
                updateCount
            );
            return new ExecuteResult(ImmutableList.of(metaResultSet));
        } catch (SQLException | SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExecuteBatchResult prepareAndExecuteBatch(
        StatementHandle sh,
        List<String> sqlCommands
    ) throws NoSuchStatementException {
        return null;
    }

    @Override
    public ExecuteBatchResult executeBatch(
        StatementHandle sh,
        List<List<TypedValue>> parameterValues
    ) throws NoSuchStatementException {
        return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Frame fetch(
        StatementHandle sh,
        long offset,
        int fetchMaxRowCount
    ) throws NoSuchStatementException, MissingResultsException {
        final DingoConnection dingoConnection = (DingoConnection) connection;
        try {
            DingoStatement stmt = dingoConnection.getStatement(sh);
            DingoResultSet resultSet = (DingoResultSet) stmt.getResultSet();
            if (resultSet == null) {
                throw new MissingResultsException(sh);
            }
            final Iterator<Object[]> iterator = resultSet.getIterator();
            final List rows = new ArrayList(fetchMaxRowCount);
            for (int i = 0; i < fetchMaxRowCount && iterator.hasNext(); ++i) {
                rows.add(Arrays.asList(iterator.next()));
            }
            boolean done = fetchMaxRowCount == 0 || !iterator.hasNext();
            return new Meta.Frame(offset, done, rows);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExecuteResult execute(
        StatementHandle sh,
        List<TypedValue> parameterValues,
        long maxRowCount
    ) throws NoSuchStatementException {
        return null;
    }

    @Override
    public ExecuteResult execute(
        StatementHandle sh,
        List<TypedValue> parameterValues,
        int maxRowsInFirstFrame
    ) throws NoSuchStatementException {
        return null;
    }

    @Override
    public MetaResultSet getTables(
        ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern,
        List<String> typeList
    ) {
        try {
            Set<Object> metaTables = DingoSchema.ROOT.getTableNames()
                .stream()
                .map(name -> Arrays.asList("DINGO", name, "TABLE"))
                .collect(Collectors.toSet());
            String[] cols = new String[]{
                "TABLE_SCHEM",
                "TABLE_NAME",
                "TABLE_TYPE",
            };
            List<ColumnMetaData> columnMetaDatas = new ArrayList<>();
            for (int i = 0; i < cols.length; i++) {
                columnMetaDatas.add(columnMetaData(cols[i], i, String.class, true));
            }
            CursorFactory cursorFactory = CursorFactory.ARRAY;
            AvaticaStatement statement = connection.createStatement();
            return MetaResultSet.create(
                ch.id,
                statement.getId(),
                true,
                new Signature(
                    columnMetaDatas,
                    "",
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    cursorFactory,
                    StatementType.SELECT
                ),
                new Frame(0, true, metaTables)
            );

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getColumns(
        ConnectionHandle ch,
        String catalog,
        Pat schemaPattern,
        Pat tableNamePattern,
        Pat columnNamePattern
    ) {
        try {
            String[] cols = new String[]{
                "COLUMN_NAME",
                "TYPE_NAME",
                "CHAR_OCTET_LENGTH",
                "DECIMAL_DIGITS",
                "NULLABLE",
                "IS_AUTOINCREMENT",
            };
            Set<Object> metaCols = Services.META.getTableDefinition(tableNamePattern.s)
                .getColumns()
                .stream()
                .map(col -> Arrays.asList(
                    col.getName(),
                    col.getType().getName(),
                    col.getPrecision(),
                    col.getScale(),
                    !col.isNotNull(),
                    false
                )).collect(Collectors.toSet());
            List<ColumnMetaData> columnMetaDatum = new ArrayList<>();
            for (int i = 0; i < cols.length; i++) {
                columnMetaDatum.add(columnMetaData(cols[i], i, String.class, true));
            }
            CursorFactory cursorFactory = CursorFactory.ARRAY;
            AvaticaStatement statement = connection.createStatement();
            return MetaResultSet.create(
                ch.id,
                statement.getId(),
                true,
                new Signature(
                    columnMetaDatum,
                    "",
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    cursorFactory,
                    StatementType.SELECT
                ),
                new Frame(0, true, metaCols)
            );

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeStatement(StatementHandle sh) {
        // Called in `AvaticaStatement.close` to do extra things.
    }

    @Override
    public boolean syncResults(
        StatementHandle sh,
        QueryState state,
        long offset
    ) throws NoSuchStatementException {
        return false;
    }

    @Override
    public void commit(ConnectionHandle ch) {
    }

    @Override
    public void rollback(ConnectionHandle ch) {
    }
}
