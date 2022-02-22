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
import io.dingodb.calcite.JobRunner;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Job;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.sql.parser.SqlParseException;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

@Slf4j
class DingoMeta extends MetaImpl {

    private final Map<Integer, Statement> statementMap = new LinkedHashMap<>();

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
        StatementHandle sh,
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
            DingoStatement statement = dingoConnection.getStatement(sh);
            statementMap.put(sh.id, statement);
            statement.setDingoSignature(signature);
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
            statementMap.put(sh.id, stmt);
            Job job = stmt.getJob();
            Enumerator<Object[]> enumerator = new JobRunner(job).createEnumerator();
            final Iterator iterator = Linq4j.enumeratorIterator(enumerator);
            final List rows = MetaImpl.collect(stmt.getCursorFactory(), iterator, new ArrayList<>());
            boolean done = fetchMaxRowCount == 0 || rows.size() < fetchMaxRowCount;
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
            String[] cols = new String[] {
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
            statementMap.put(statement.getId(), statement);
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
            String[] cols = new String[] {
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
            List<ColumnMetaData> columnMetaDatas = new ArrayList<>();
            for (int i = 0; i < cols.length; i++) {
                columnMetaDatas.add(columnMetaData(cols[i], i, String.class, true));
            }
            CursorFactory cursorFactory = CursorFactory.ARRAY;
            AvaticaStatement statement = connection.createStatement();
            statementMap.put(statement.getId(), statement);
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
                new Frame(0, true, metaCols)
            );

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeStatement(StatementHandle sh) {
        Statement statement = statementMap.remove(sh.id);
        if (statement == null) {
            //log.warn("Statement is null, connection id [{}], statement id [{}].", sh.connectionId, sh.id);
        } else {
            try {
                statement.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
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
