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
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.MetaCache;
import io.dingodb.common.driver.DingoMetaPrimaryKey;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.converter.AvaticaResultSetConverter;
import io.dingodb.exec.JobRunner;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.impl.JobManagerImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaClientRuntimeException;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
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
import org.apache.calcite.sql.parser.SqlParseException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;

import static io.dingodb.calcite.DingoTable.dingo;
import static io.dingodb.common.error.DingoException.CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP;
import static io.dingodb.common.error.DingoException.RUNTIME_EXCEPTION_PATTERN_CODE_MAP;
import static io.dingodb.common.error.DingoException.TYPE_CAST_ERROR;
import static java.util.Objects.requireNonNull;

@Slf4j
public class DingoMeta extends MetaImpl {
    private static final JobManager jobManager = JobManagerImpl.INSTANCE;

    public DingoMeta(DingoConnection connection) {
        super(connection);
    }

    @Nonnull
    private static Predicate<String> patToFilter(@Nonnull Pat pat) {
        if (pat.s == null) {
            return str -> true;
        }
        StringBuilder buf = new StringBuilder("^");
        char[] charArray = pat.s.toCharArray();
        int backslashIndex = -2;
        for (int i = 0; i < charArray.length; i++) {
            char c = charArray[i];
            if (backslashIndex == i - 1) {
                buf.append('[').append(c).append(']');
                continue;
            }
            switch (c) {
                case '\\':
                    backslashIndex = i;
                    break;
                case '%':
                    buf.append(".*");
                    break;
                case '_':
                    buf.append(".");
                    break;
                case '[':
                    buf.append("\\[");
                    break;
                case ']':
                    buf.append("\\]");
                    break;
                default:
                    buf.append('[').append(c).append(']');
                    break;
            }
        }
        buf.append("$");
        Pattern regex = Pattern.compile(buf.toString());
        return str -> regex.matcher(str).matches();
    }

    @Nonnull
    private static Id jobIdFromSh(@Nonnull StatementHandle sh) {
        return new Id(sh.toString());
    }

    @Nonnull
    private static Iterator<Object[]> createIterator(@Nonnull AvaticaStatement statement) {
        Signature signature = statement.handle.signature;
        Iterator<Object[]> iterator;
        if (signature instanceof DingoExplainSignature) {
            DingoExplainSignature explainSignature = (DingoExplainSignature) signature;
            iterator = ImmutableList.of(new Object[]{explainSignature.toString()}).iterator();
        } else {
            Job job = jobManager.getJob(jobIdFromSh(statement.handle));
            if (statement instanceof DingoPreparedStatement) {
                DingoPreparedStatement dingoPreparedStatement = (DingoPreparedStatement) statement;
                try {
                    Object[] parasValue = TypedValue.values(dingoPreparedStatement.getParameterValues()).toArray();
                    job.setParas(parasValue);
                } catch (NullPointerException e) {
                    throw new IllegalStateException("Not all parameters are set.");
                }
            }
            iterator = new JobRunner(job).createIterator();
        }
        return iterator;
    }

    @Nonnull
    static ExecuteResult createExecuteResult(@Nonnull StatementHandle sh) {
        MetaResultSet metaResultSet;
        final Frame frame = new Frame(0, false, Collections.emptyList());
        metaResultSet = MetaResultSet.create(sh.connectionId, sh.id, false, sh.signature, frame);
        return new ExecuteResult(ImmutableList.of(metaResultSet));
    }

    static int getUpdateCount(@Nonnull StatementType statementType) {
        final int updateCount;
        switch (statementType) {
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
        return updateCount;
    }

    private static Collection<CalciteSchema> getMatchedSubSchema(
        @Nonnull CalciteSchema rootSchema,
        @Nonnull Pat pat
    ) {
        final Predicate<String> filter = patToFilter(pat);
        return rootSchema.getSubSchemaMap().entrySet().stream()
            .filter(e -> filter.test(e.getKey()))
            .map(Map.Entry::getValue)
            .collect(Collectors.toSet());
    }

    private static Collection<CalciteSchema.TableEntry> getMatchedTables(
        @Nonnull Collection<CalciteSchema> schemas,
        @Nonnull Pat pat
    ) {
        final Predicate<String> filter = patToFilter(pat);
        return schemas.stream()
            .flatMap(s -> s.getTableNames().stream()
                .filter(filter)
                .map(n -> s.getTable(n, false)))
            .collect(Collectors.toList());
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
            sh.signature = parser.parseQuery(jobManager, jobIdFromSh(sh), sql, context);
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
        DingoConnection dingoConnection = (DingoConnection) connection;
        DingoConnection.DingoContext context = dingoConnection.createContext();
        DingoDriverParser parser = new DingoDriverParser(context.getParserContext());
        try {
            final Timer.Context timeCtx = DingoMetrics.getTimeContext("parse_query");
            final Signature signature = parser.parseQuery(jobManager, jobIdFromSh(sh), sql, context);
            timeCtx.stop();
            sh.signature = signature;
            final int updateCount = getUpdateCount(signature.statementType);
            synchronized (callback.getMonitor()) {
                callback.clear();
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
    ) {
        return null;
    }

    @Override
    public ExecuteBatchResult executeBatch(
        StatementHandle sh,
        List<List<TypedValue>> parameterValues
    ) {
        return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Frame fetch(
        StatementHandle sh,
        long offset,
        int fetchMaxRowCount
    ) throws NoSuchStatementException, MissingResultsException {
        final long startTime = System.currentTimeMillis();
        AvaticaStatement statement = ((DingoConnection) connection).getStatement(sh);
        try {
            DingoResultSet resultSet = (DingoResultSet) statement.getResultSet();
            if (resultSet == null) {
                throw new MissingResultsException(sh);
            }
            Signature signature = resultSet.getSignature();
            Iterator<Object[]> iterator = resultSet.getIterator();
            if (iterator == null) {
                iterator = createIterator(statement);
                resultSet.setIterator(iterator);
            }
            final List rows = new ArrayList(fetchMaxRowCount);
            DingoType dingoType = DingoTypeFactory.fromColumnMetaDataList(signature.columns);
            for (int i = 0; i < fetchMaxRowCount && iterator.hasNext(); ++i) {
                rows.add(dingoType.convertTo(iterator.next(), AvaticaResultSetConverter.INSTANCE));
            }
            boolean done = fetchMaxRowCount == 0 || !iterator.hasNext();
            return new Frame(offset, done, rows);
        } catch (SQLException e) {
            log.error("Fetch catch exception:{}", e, e);
            throw new RuntimeException(e);
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
        @Nonnull StatementHandle sh,
        List<TypedValue> parameterValues,
        int maxRowsInFirstFrame
    ) throws NoSuchStatementException {
        // parameterValues are not used here, actually they are set to statement before call this function.
        DingoPreparedStatement statement = (DingoPreparedStatement) ((DingoConnection) connection).getStatement(sh);
        if (statement.getStatementType().canUpdate()) {
            final Iterator<Object[]> iterator = createIterator(statement);
            MetaResultSet metaResultSet = MetaResultSet.count(
                sh.connectionId,
                sh.id,
                ((Number) iterator.next()[0]).longValue()
            );
            return new ExecuteResult(ImmutableList.of(metaResultSet));
        }
        return createExecuteResult(sh);
    }

    @Override
    public void closeStatement(@Nonnull StatementHandle sh) {
        // Called in `AvaticaStatement.close` to do extra things.
        AvaticaStatement statement = connection.statementMap.get(sh.id);
        if (statement != null) {
            try {
                statement.close();
                jobManager.removeJob(jobIdFromSh(sh));
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
    ) {
        return false;
    }

    @Override
    public void commit(ConnectionHandle ch) {
    }

    @Override
    public void rollback(ConnectionHandle ch) {
    }

    @Override
    public MetaResultSet getTables(
        ConnectionHandle ch,
        String catalog,
        @Nonnull Pat schemaPattern,
        Pat tableNamePattern,
        List<String> typeList
    ) {
        MetaCache.initTableDefinitions();
        final DingoConnection dingoConnection = (DingoConnection) connection;
        final DingoParserContext context = dingoConnection.getContext();
        final CalciteSchema rootSchema = context.getRootSchema();
        final Collection<CalciteSchema> schemas = getMatchedSubSchema(rootSchema, schemaPattern);
        final Collection<CalciteSchema.TableEntry> tables = getMatchedTables(schemas, tableNamePattern);
        return createResultSet(
            Linq4j.asEnumerable(tables)
                .select(t -> new MetaTable(
                    catalog,
                    t.schema.name,
                    t.name,
                    t.getTable().getJdbcTableType().jdbcName
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
        @Nonnull Pat columnNamePattern
    ) {
        MetaCache.initTableDefinitions();
        final DingoConnection dingoConnection = (DingoConnection) connection;
        final DingoParserContext context = dingoConnection.getContext();
        final CalciteSchema rootSchema = context.getRootSchema();
        final Collection<CalciteSchema> schemas = getMatchedSubSchema(rootSchema, schemaPattern);
        final Collection<CalciteSchema.TableEntry> tables = getMatchedTables(schemas, tableNamePattern);
        final Predicate<String> filter = patToFilter(columnNamePattern);
        List<MetaColumn> columns = tables.stream()
            .flatMap(t -> {
                RelDataType rowType = t.getTable().getRowType(context.getTypeFactory());
                return rowType.getFieldList().stream()
                    .filter(f -> filter.test(f.getName()))
                    .map(f -> {
                        final int precision = f.getType().getSqlTypeName().allowsPrec()
                            && !(f.getType() instanceof RelDataTypeFactoryImpl.JavaType)
                            ? f.getType().getPrecision()
                            : RelDataType.PRECISION_NOT_SPECIFIED;
                        final Integer scale = f.getType().getSqlTypeName().allowsScale()
                            ? f.getType().getScale()
                            : null;
                        return new MetaColumn(
                            catalog,
                            t.schema.name,
                            t.name,
                            f.getName(),
                            f.getType().getSqlTypeName().getJdbcOrdinal(),
                            f.getType().getFullTypeString(),
                            precision,
                            scale,
                            10,
                            f.getType().isNullable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls,
                            precision,
                            f.getIndex() + 1,
                            f.getType().isNullable() ? "YES" : "NO"
                        );
                    });
            })
            .collect(Collectors.toList());
        return createResultSet(
            Linq4j.asEnumerable(columns),
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
        final DingoConnection dingoConnection = (DingoConnection) connection;
        final DingoParserContext context = dingoConnection.getContext();
        final CalciteSchema rootSchema = context.getRootSchema();
        final Collection<CalciteSchema> schemas = getMatchedSubSchema(rootSchema, schemaPattern);
        return createResultSet(
            Linq4j.asEnumerable(schemas)
                .select(schema -> new MetaSchema(catalog, schema.name)),
            MetaSchema.class,
            "TABLE_SCHEM",
            "TABLE_CATALOG");
    }

    @Override
    public MetaResultSet getPrimaryKeys(ConnectionHandle ch, String catalog, String schemaName, String tableName) {
        final DingoConnection dingoConnection = (DingoConnection) connection;
        final DingoParserContext context = dingoConnection.getContext();
        final CalciteSchema rootSchema = context.getRootSchema();
        final CalciteSchema schema = rootSchema.getSubSchema(schemaName, false);
        final CalciteSchema.TableEntry table = schema.getTable(tableName, false);
        final DingoTable dingoTable = dingo(table.getTable());
        final TableDefinition tableDefinition = dingoTable.getTableDefinition();
        final TupleMapping mapping = tableDefinition.getKeyMapping();
        return createResultSet(
            Linq4j.asEnumerable(IntStream.range(0, mapping.size())
                .mapToObj(i -> {
                    ColumnDefinition c = tableDefinition.getColumn(mapping.get(i));
                    return new DingoMetaPrimaryKey(
                        catalog,
                        schemaName,
                        tableName,
                        c.getName(),
                        (short) (i + 1)
                    );
                })
                .collect(Collectors.toList())
            ),
            DingoMetaPrimaryKey.class,
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "COLUMN_NAME",
            "KEY_SEQ"
        );
    }
}
