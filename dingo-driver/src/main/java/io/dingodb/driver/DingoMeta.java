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

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.MetaCache;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.converter.AvaticaResultSetConverter;
import io.dingodb.exec.operator.RootOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaClientRuntimeException;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.parser.SqlParseException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

import static io.dingodb.common.error.DingoException.CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP;
import static io.dingodb.common.error.DingoException.RUNTIME_EXCEPTION_PATTERN_CODE_MAP;
import static io.dingodb.common.error.DingoException.TYPE_CAST_ERROR;
import static java.util.Objects.requireNonNull;

@Slf4j
public class DingoMeta extends MetaImpl {
    public DingoMeta(DingoConnection connection) {
        super(connection);
    }

    public AvaticaStatement getStatement(StatementHandle sh) throws SQLException {
        return ((DingoConnection) connection).getStatement(sh);
    }

    @Override
    public StatementHandle prepare(
        ConnectionHandle ch,
        String sql,
        long maxRowCount
    ) {
        final StatementHandle sh = createStatement(ch);
        DingoConnection dingoConnection = (DingoConnection) connection;
        try {
            DingoConnection.DingoContext context = dingoConnection.createContext();
            DingoDriverParser parser = new DingoDriverParser(context.getParserContext());
            sh.signature = parser.parseQuery(sql, context);
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
        return sh;
    }

    @Deprecated
    @Override
    public ExecuteResult prepareAndExecute(
        StatementHandle sh,
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
        @Nonnull PrepareCallback callback
    ) {
        final long startTime = System.currentTimeMillis();
        try {
            DingoConnection dingoConnection = (DingoConnection) connection;
            DingoConnection.DingoContext context = dingoConnection.createContext();
            DingoDriverParser parser = new DingoDriverParser(context.getParserContext());
            final Timer.Context timeCtx = DingoMetrics.getTimeContext("parse_query");
            final DingoSignature signature = parser.parseQuery(sql, context);
            timeCtx.stop();
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
            checkJobHasFailed(signature);
            return new ExecuteResult(ImmutableList.of(metaResultSet));
        } catch (SQLException | SqlParseException | RuntimeException e) {
            Throwable throwable = e;
            log.error("Catch execute exception: ", throwable);
            while (throwable instanceof CompletionException || throwable instanceof InvocationTargetException) {
                if (throwable.getCause() == null) {
                    break;
                }
                throwable = throwable.getCause();
            }
            String exceptMessage;
            Integer exceptionCode = -1;
            if (throwable instanceof CalciteContextException) {
                exceptMessage = (((CalciteContextException) throwable).getMessage());
                for (Pattern pat : CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP.keySet()) {
                    if (pat.matcher(exceptMessage).find()) {
                        exceptionCode = CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP.get(pat);
                        break;
                    }
                }
                throw new AvaticaClientRuntimeException(exceptMessage, exceptionCode,
                    Service.ErrorResponse.UNKNOWN_SQL_STATE, AvaticaSeverity.ERROR,
                    Collections.singletonList(""), null);
            } else if (throwable instanceof RuntimeException) {
                if (throwable.getCause() == null) {
                    exceptMessage = throwable.getMessage() != null ? throwable.getMessage() :
                        "Null pointer";
                } else {
                    exceptMessage = throwable.getCause().getMessage();
                }
                for (Pattern pat : RUNTIME_EXCEPTION_PATTERN_CODE_MAP.keySet()) {
                    if (pat.matcher(exceptMessage).find() || (pat.matcher(throwable.getMessage()).find()
                        && !throwable.getMessage().contains("CAST"))) {
                        exceptionCode = RUNTIME_EXCEPTION_PATTERN_CODE_MAP.get(pat);
                        // TODO: Refine error message.
                        if (exceptionCode == TYPE_CAST_ERROR) {
                            exceptMessage = throwable.getMessage();
                        }
                        break;
                    }
                }
                throw new AvaticaClientRuntimeException(exceptMessage, exceptionCode,
                    Service.ErrorResponse.UNKNOWN_SQL_STATE, AvaticaSeverity.ERROR,
                    Collections.singletonList(""), null);
            } else if (throwable instanceof SQLException) {
                throw new AvaticaClientRuntimeException(throwable.toString(), 12,
                    Service.ErrorResponse.UNKNOWN_SQL_STATE,
                    AvaticaSeverity.ERROR, Collections.singletonList(""), null);
            } else {
                throw new AvaticaClientRuntimeException(throwable.toString(), 13,
                    Service.ErrorResponse.UNKNOWN_SQL_STATE,
                    AvaticaSeverity.ERROR, Collections.singletonList(""), null);
            }

        } finally {
            if (log.isDebugEnabled()) {
                log.debug("DingoMeta prepareAndExecute total cost: {}ms.", System.currentTimeMillis() - startTime);
            }
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
    ) throws MissingResultsException {
        final long startTime = System.currentTimeMillis();
        try {
            AvaticaStatement stmt = getStatement(sh);
            DingoResultSet resultSet = (DingoResultSet) stmt.getResultSet();
            final Iterator<Object[]> iterator = resultSet.getIterator();
            final List rows = new ArrayList(fetchMaxRowCount);
            DingoSignature signature;
            if (stmt instanceof DingoStatement) {
                signature = ((DingoStatement) stmt).getSignature();
            } else if (stmt instanceof DingoPreparedStatement) {
                signature = ((DingoPreparedStatement) stmt).getSignature();
            } else {
                throw new IllegalStateException("Got an not-dingo statement, this is an internal error.");
            }
            DingoType dingoType = DingoTypeFactory.fromColumnMetaDataList(signature.columns);
            for (int i = 0; i < fetchMaxRowCount && iterator.hasNext(); ++i) {
                rows.add(dingoType.convertTo(iterator.next(), AvaticaResultSetConverter.INSTANCE));
            }
            boolean done = fetchMaxRowCount == 0 || !iterator.hasNext();
            checkJobHasFailed(signature);
            return new Meta.Frame(offset, done, rows);
        } catch (SQLException e) {
            log.error("Fetch catch exception:{}", e, e);
            // TODO AvaticaClientRuntimeException
            throw new RuntimeException(e.getMessage());
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("DingoMeta fetch, cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Deprecated
    @Override
    public ExecuteResult execute(
        StatementHandle sh,
        List<TypedValue> parameterValues,
        long maxRowCount
    ) {
        return null;
    }

    @Override
    public ExecuteResult execute(
        StatementHandle sh,
        List<TypedValue> parameterValues,
        int maxRowsInFirstFrame
    ) {
        final DingoConnection dingoConnection = (DingoConnection) connection;
        try {
            DingoPreparedStatement statement = (DingoPreparedStatement) dingoConnection.getStatement(sh);
            final DingoSignature signature = (DingoSignature) statement.getSignature();
            MetaResultSet metaResultSet;
            final Meta.Frame frame = new Meta.Frame(0, false, Collections.emptyList());
            metaResultSet = MetaResultSet.create(sh.connectionId, sh.id, false, signature, frame);
            return new ExecuteResult(ImmutableList.of(metaResultSet));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeStatement(@Nonnull StatementHandle sh) {
        // Called in `AvaticaStatement.close` to do extra things.
        AvaticaStatement statement = connection.statementMap.get(sh.id);
        if (statement != null) {
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

    private void checkJobHasFailed(@Nonnull DingoSignature signature) throws SQLException {
        /**
         * when the operation is `Create` or `Drop`, then the signature.getJob is null.
         */
        if (signature.getJob() != null) {
            RootOperator rootOperator = signature.getJob().getRootTask().getRoot();
            if (rootOperator.getErrorFin() != null) {
                String errorMsg = (rootOperator.getErrorFin()).detail();
                log.warn("Check Job:{} operator:{} has failed, ErrorMsg: {}",
                    rootOperator.getTask().getJobId().toString(),
                    rootOperator.getId().toString(),
                    errorMsg);
                throw new SQLException(errorMsg);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <E> MetaResultSet createResultSet(
        Enumerable<E> enumerable,
        Class<E> clazz,
        String... names
    ) {
        requireNonNull(names, "names");
        final List<ColumnMetaData> columns = new ArrayList<>(names.length);
        final List<Field> fields = new ArrayList<>(names.length);
        final List<String> fieldNames = new ArrayList<>(names.length);
        for (String name : names) {
            final int index = fields.size();
            final String fieldName = AvaticaUtils.toCamelCase(name);
            final Field field;
            try {
                field = clazz.getField(fieldName);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
            }
            columns.add(columnMetaData(name, index, field.getType(), false));
            fields.add(field);
            fieldNames.add(fieldName);
        }
        final Iterable<Object> iterable = (Iterable<Object>) enumerable;
        return createResultSet(Collections.emptyMap(),
            columns, CursorFactory.record(clazz, fields, fieldNames),
            new Frame(0, true, iterable));
    }

    @Override
    public MetaResultSet getTables(
        ConnectionHandle ch,
        String catalog,
        @Nonnull Pat schemaPattern,
        Pat tableNamePattern,
        List<String> typeList
    ) {
        DingoConnection dingoConnection = (DingoConnection) connection;
        DingoParserContext context = dingoConnection.getContext();
        CalciteSchema rootSchema = context.getRootSchema();
        // TODO: should match by pattern
        String schemaName = schemaPattern.s == null ? context.getDefaultSchemaName() : schemaPattern.s;
        CalciteSchema schema = rootSchema.getSubSchema(schemaName, false);
        if (schema == null) {
            return createEmptyResultSet(MetaTable.class);
        }
        MetaCache.initTableDefinitions();
        return createResultSet(
            Linq4j.asEnumerable(schema.getTableNames())
                .select(name -> new MetaTable(
                    catalog,
                    schemaName,
                    name,
                    schema.getTable(name, true).getTable().getJdbcTableType().jdbcName
                )),
            MetaTable.class,
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "TABLE_TYPE"
        );
    }

    @Override
    public MetaResultSet getColumns(
        ConnectionHandle ch,
        String catalog,
        @Nonnull Pat schemaPattern,
        Pat tableNamePattern,
        Pat columnNamePattern
    ) {
        DingoConnection dingoConnection = (DingoConnection) connection;
        DingoParserContext context = dingoConnection.getContext();
        CalciteSchema rootSchema = context.getRootSchema();
        // TODO: should match by pattern
        String schemaName = schemaPattern.s == null ? context.getDefaultSchemaName() : schemaPattern.s;
        CalciteSchema schema = rootSchema.getSubSchema(schemaName, false);
        if (schema == null || tableNamePattern.s == null) {
            return createEmptyResultSet(MetaColumn.class);
        }
        CalciteSchema.TableEntry tableEntry = schema.getTable(tableNamePattern.s, false);
        if (tableEntry == null) {
            return createEmptyResultSet(MetaColumn.class);
        }
        Table table = tableEntry.getTable();
        RelDataType rowType = table.getRowType(dingoConnection.getContext().getTypeFactory());
        return createResultSet(
            Linq4j.asEnumerable(rowType.getFieldList())
                .select(field -> {
                    final int precision =
                        field.getType().getSqlTypeName().allowsPrec()
                            && !(field.getType()
                            instanceof RelDataTypeFactoryImpl.JavaType)
                            ? field.getType().getPrecision()
                            : -1;
                    return new MetaColumn(
                        catalog,
                        schemaPattern.s,
                        tableNamePattern.s,
                        field.getName(),
                        field.getType().getSqlTypeName().getJdbcOrdinal(),
                        field.getType().getFullTypeString(),
                        precision,
                        field.getType().getSqlTypeName().allowsScale()
                            ? field.getType().getScale()
                            : null,
                        10,
                        field.getType().isNullable()
                            ? DatabaseMetaData.columnNullable
                            : DatabaseMetaData.columnNoNulls,
                        precision,
                        field.getIndex() + 1,
                        field.getType().isNullable() ? "YES" : "NO");
                }),
            MetaColumn.class,
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "COLUMN_NAME",
            "DATA_TYPE",
            "TYPE_NAME",
            "COLUMN_SIZE",
            "DECIMAL_DIGITS",
            "NUM_PREC_RADIX",
            "NULLABLE",
            "CHAR_OCTET_LENGTH",
            "ORDINAL_POSITION",
            "IS_NULLABLE",
            "IS_AUTOINCREMENT",
            "IS_GENERATEDCOLUMN"
        );
    }

    @Override
    public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
        DingoConnection dingoConnection = (DingoConnection) connection;
        CalciteSchema rootSchema = dingoConnection.getContext().getRootSchema();
        // TODO: filter by pattern.
        return createResultSet(
            Linq4j.asEnumerable(rootSchema.getSubSchemaMap().values())
                .select(schema -> new MetaSchema(catalog, schema.getName())),
            MetaSchema.class,
            "TABLE_SCHEM",
            "TABLE_CATALOG");
    }
}
