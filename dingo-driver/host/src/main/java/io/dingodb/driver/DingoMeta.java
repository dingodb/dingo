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
import com.google.common.primitives.Longs;
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.schema.DingoSchema;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.common.CommonId;
import io.dingodb.common.exception.DingoSqlException;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.table.IndexScan;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.Optional;
import io.dingodb.driver.type.converter.AvaticaResultSetConverter;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.exception.TaskFinException;
import io.dingodb.exec.fin.ErrorType;
import io.dingodb.exec.impl.JobIteratorImpl;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import io.dingodb.verify.privilege.PrivilegeVerify;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Field;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

@Slf4j
public class DingoMeta extends MetaImpl {
    @Getter
    private final JobManager jobManager;

    public DingoMeta(DingoConnection connection, JobManager jobManager) {
        super(connection);
        this.jobManager = jobManager;
    }

    @NonNull
    private static Predicate<String> patToFilter(@NonNull Pat pat, boolean caseInSensitive) {
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
        Pattern regex;
        if (caseInSensitive) {
            regex = Pattern.compile(buf.toString(), Pattern.CASE_INSENSITIVE);
        } else {
            regex = Pattern.compile(buf.toString());
        }
        return str -> regex.matcher(str).matches();
    }

    static int getUpdateCount(@NonNull StatementType statementType) {
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

    private Collection<CalciteSchema> getMatchedSubSchema(
        @NonNull CalciteSchema usedSchema,
        @NonNull Pat pat
    ) {
        final Predicate<String> filter = patToFilter(pat, true);
        if (usedSchema.getSubSchemaMap().isEmpty()) {
            return Collections.emptyList();
        }
        return usedSchema.getSubSchemaMap().values().stream()
            .flatMap(s -> Stream.concat(getMatchedSubSchema(s, pat).stream(), Stream.of(s)))
            .filter(s -> filter.test(((DingoSchema) s.schema).name()))
            .filter(s -> verifyPrivilege(((DingoSchema) s.schema)))
            .collect(Collectors.toSet());
    }

    private Collection<CalciteSchema.TableEntry> getMatchedTables(
        @NonNull Collection<CalciteSchema> schemas,
        @NonNull Pat pat
    ) {
        final Predicate<String> filter = patToFilter(pat, true);
        return schemas.stream()
            .flatMap(s -> s.getTableNames().stream()
                .filter(filter)
                .filter(name -> verifyPrivilege((DingoSchema) s.schema, name, "getTables"))
                .map(name -> s.getTable(name, false)))
            .collect(Collectors.toList());
    }

    private boolean verifyPrivilege(DingoSchema schema) {
        return verifyPrivilege(schema, null, "getSchemas");
    }

    private boolean verifyPrivilege(DingoSchema schema, String tableName, String command) {
        try {
            DingoConnection dingoConnection = (DingoConnection) connection;
            String user = dingoConnection.getContext().getOption("user");
            String host = dingoConnection.getContext().getOption("host");
            return PrivilegeVerify.verify(user, host, schema.name(), tableName, command);
        } catch (Exception e) {
            return true;
        }
    }

    @NonNull
    private Iterator<Object[]> createIterator(@NonNull AvaticaStatement statement) {
        if (statement instanceof DingoStatement) {
            return ((DingoStatement) statement).createIterator(jobManager);
        } else if (statement instanceof DingoPreparedStatement) {
            return ((DingoPreparedStatement) statement).createIterator(jobManager);
        }
        throw new IllegalStateException(
            "Statement \"" + statement.handle + "\" has wrong type: \""
                + statement.getClass().getName() + "\"."
        );
    }

    private <E> MetaResultSet createArrayResultSet(
        Enumerable enumerable,
        Class[] classes,
        String... names
    ) {
        requireNonNull(names, "names");
        final List<ColumnMetaData> columns = new ArrayList<>(names.length);
        for (int i = 0; i < names.length; i ++) {
            columns.add(columnMetaData(names[i], i, classes[i], false));
        }
        final Iterable<Object> iterable = (Iterable<Object>) enumerable;
        return createResultSet(Collections.emptyMap(),
            columns, Meta.CursorFactory.ARRAY,
            new Frame(0, true, iterable));
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
        DingoDriverParser parser = new DingoDriverParser(dingoConnection);
        sh.signature = parser.parseQuery(jobManager, sh.toString(), sql);
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
        @NonNull StatementHandle sh,
        String sql,
        long maxRowCount,
        int maxRowsInFirstFrame,
        @NonNull PrepareCallback callback
    ) {
        final long startTime = System.currentTimeMillis();
        DingoConnection dingoConnection = (DingoConnection) connection;
        DingoDriverParser parser = new DingoDriverParser(dingoConnection);
        try {
            DingoStatement statement = (DingoStatement) dingoConnection.getStatement(sh);
            statement.removeJob(jobManager);
            final Timer.Context timeCtx = DingoMetrics.getTimeContext("parse_query");
            Meta.Signature signature = parser.parseQuery(jobManager, sh.toString(), sql);
            // for mysql protocol start
            statement.setInTransaction(parser.isInTransaction());
            statement.setAutoCommit(connection.getAutoCommit());
            String tranReadOnly = connection.getClientInfo("transaction_read_only");
            tranReadOnly = tranReadOnly == null ? "off" : tranReadOnly;
            statement.setTransReadOnly(tranReadOnly.equalsIgnoreCase("on"));
            // for mysql protocol end
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
        } catch (Throwable e) {
            log.error("Prepare and execute error, sql: <[{}]>.", sql, e);
            throw ExceptionUtils.toRuntime(e);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("DingoMeta prepareAndExecute total cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    @Override
    public ExecuteBatchResult prepareAndExecuteBatch(
        StatementHandle sh,
        @NonNull List<String> sqlCommands
    ) throws NoSuchStatementException {
        DingoStatement statement = (DingoStatement) ((DingoConnection) connection).getStatement(sh);
        final List<Long> updateCounts = new ArrayList<>();
        final PrepareCallback callback = new PrepareCallback() {
            long updateCount;
            @Nullable Signature signature;

            @Override
            public Object getMonitor() {
                return statement;
            }

            @Override
            public void clear() {
            }

            @Override
            public void assign(
                Signature signature,
                @Nullable Frame firstFrame,
                long updateCount
            ) {
                statement.setSignature(signature);
                this.signature = signature;
                this.updateCount = updateCount;
            }

            @Override
            public void execute() {
                assert signature != null;
                if (signature.statementType.canUpdate()) {
                    final Iterator<Object[]> iterator = createIterator(statement);
                    updateCount = ((Number) iterator.next()[0]).longValue();
                }
                updateCounts.add(updateCount);
            }
        };
        for (String sqlCommand : sqlCommands) {
            prepareAndExecute(sh, sqlCommand, -1L, -1, callback);
        }
        return new ExecuteBatchResult(Longs.toArray(updateCounts));
    }

    @Override
    public ExecuteBatchResult executeBatch(
        StatementHandle sh,
        @NonNull List<List<TypedValue>> parameterValues
    ) throws NoSuchStatementException {
        final List<Long> updateCounts = new ArrayList<>();
        ITransaction transaction = ((DingoConnection) connection).getTransaction();
        DingoPreparedStatement statement = (DingoPreparedStatement) ((DingoConnection) connection).getStatement(sh);
        if (transaction == null) {
            transaction = prepareJobAndTxn(sh, statement);
        }
        try {
            for (List<TypedValue> parameterValue : parameterValues) {
                ExecuteResult executeResult = execBatch(sh, parameterValue, -1);
                final long updateCount =
                    executeResult.resultSets.size() == 1
                        ? executeResult.resultSets.get(0).updateCount
                        : -1L;
                updateCounts.add(updateCount);
            }
        } catch (Throwable throwable) {
            log.error("run job exception:{}", throwable, throwable);
            if (transaction != null && transaction.isPessimistic()
                && transaction.getPrimaryKeyLock() != null
                && statement.isDml()) {
                // rollback pessimistic lock
                transaction.rollBackPessimisticLock(jobManager);
            }
            if (transaction != null) {
                transaction.addSql(statement.getSql());
                if (transaction.getType() == TransactionType.NONE || transaction.isAutoCommit()) {
                    try {
                        cleanTransaction();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            throw ExceptionUtils.toRuntime(throwable);
        }
        if (transaction != null) {
            transaction.addSql(statement.getSql());
            if (transaction.getType() == TransactionType.NONE || transaction.isAutoCommit()) {
                try {
                    connection.commit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return new ExecuteBatchResult(Longs.toArray(updateCounts));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Frame fetch(
        StatementHandle sh,
        long offset,
        int fetchMaxRowCount
    ) throws NoSuchStatementException {
        return getFrame(sh, offset, fetchMaxRowCount);
    }

    @NonNull
    private Frame getFrame(StatementHandle sh, long offset, int fetchMaxRowCount) throws NoSuchStatementException {
        final long startTime = System.currentTimeMillis();
        AvaticaStatement statement = ((DingoConnection) connection).getStatement(sh);
        try {
            DingoResultSet resultSet = (DingoResultSet) statement.getResultSet();
            if (resultSet == null) {
                throw new MissingResultsException(sh);
            }
            Signature signature = resultSet.getSignature();
            Iterator<Object[]> iterator = resultSet.getIterator();
            ITransaction transaction = ((DingoConnection) connection).getTransaction();
            final List rows = new ArrayList(fetchMaxRowCount);
            boolean done = false;
            try {
                if (iterator == null) {
                    iterator = createIterator(statement);
                    resultSet.setIterator(iterator);
                }
                DingoType dingoType = DefinitionMapper.mapToDingoType(signature.columns);
                AvaticaResultSetConverter converter = new AvaticaResultSetConverter(resultSet.getLocalCalendar());
                for (int i = 0; i < fetchMaxRowCount && iterator.hasNext(); ++i) {
                    rows.add(dingoType.convertTo(iterator.next(), converter));
                }
                if (iterator instanceof JobIteratorImpl) {
                    JobIteratorImpl jobIterator = (JobIteratorImpl) iterator;
                    boolean hasIncId;
                    Long autoIncId = null;
                    if (jobIterator.getAutoIncId() == null) {
                        hasIncId = false;
                    } else {
                        hasIncId = true;
                        autoIncId = jobIterator.getAutoIncId();;
                    }
                    if (statement instanceof DingoStatement) {
                        ((DingoStatement) statement).setHasIncId(hasIncId);
                        ((DingoStatement) statement).setAutoIncId(autoIncId);
                    } else if (statement instanceof DingoPreparedStatement) {
                        ((DingoPreparedStatement) statement).setHasIncId(hasIncId);
                        ((DingoPreparedStatement) statement).setAutoIncId(autoIncId);
                    }
                }
            } catch (Throwable e) {
                log.error("run job exception:{}", e, e);
                if (transaction != null && transaction.isPessimistic() && transaction.getPrimaryKeyLock() != null
                    && (sh.signature.statementType == StatementType.DELETE
                    || sh.signature.statementType == StatementType.INSERT
                    || sh.signature.statementType == StatementType.UPDATE
                    || sh.signature.statementType == StatementType.IS_DML)) {
                    // rollback pessimistic lock
                    transaction.rollBackPessimisticLock(jobManager);
                }
                if (transaction != null) {
                    transaction.addSql(signature.sql);
                    if (transaction.getType() == TransactionType.NONE || transaction.isAutoCommit()) {
                        cleanTransaction();
                    }
                }
                if (e instanceof DingoSqlException) {
                    if (((DingoSqlException) e).getSqlCode() == 3024
                        & ((DingoSqlException) e).getSqlState().equals("HY000")) {
                        if (statement instanceof DingoPreparedStatement) {
                            CommonId jobId = ((DingoPreparedStatement) statement).getJobId(jobManager);
                            jobManager.removeJob(jobId);
                        } else if (statement instanceof DingoStatement) {
                            ((DingoStatement) statement).removeJob(jobManager);
                        }
                    }
                }
                throw ExceptionUtils.toRuntime(e);
            }
            done = fetchMaxRowCount == 0 || !iterator.hasNext();
            if (transaction != null) {
                log.info("{} sql:{} , txnAutoCommit:{}, txnType:{} ", transaction.getTxnId(),
                    signature.sql, transaction.isAutoCommit(), transaction.getType());
                transaction.addSql(signature.sql);
                if (transaction.getType() == TransactionType.NONE || transaction.isAutoCommit()) {
                    try {
                        connection.commit();
                    } catch (TaskFinException e1) {
                        log.info(e1.getMessage(), e1);
                        if (e1.getErrorType().equals(ErrorType.WriteConflict)) {
                            return requireNonNull(
                                resolveWriteConflict(
                                sh,
                                offset,
                                fetchMaxRowCount,
                                statement,
                                resultSet,
                                signature,
                                transaction,
                                e1)
                            );
                        } else {
                            throw e1;
                        }
                    } catch (WriteConflictException e2) {
                        return requireNonNull(
                            resolveWriteConflict(
                                sh,
                                offset,
                                fetchMaxRowCount,
                                statement,
                                resultSet,
                                signature,
                                transaction,
                                e2)
                        );
                    }
                }
            }
            return new Frame(offset, done, rows) ;
        } catch (Throwable e) {
            log.error("Fetch catch exception:{}", e, e);
            throw ExceptionUtils.toRuntime(e);
        } finally {
            ((DingoConnection) connection).setCommandStartTime(0);
            if (log.isDebugEnabled()) {
                log.debug("DingoMeta fetch, cost: {}ms.", System.currentTimeMillis() - startTime);
            }
        }
    }

    private Frame resolveWriteConflict(StatementHandle sh, long offset, int fetchMaxRowCount,
                                       AvaticaStatement statement, DingoResultSet resultSet,
                                       Signature signature, ITransaction transaction,
                                       RuntimeException exception) throws SQLException, NoSuchStatementException {
        int txnRetryLimit = getTxnRetryLimit();
        log.info("retry txnRetryLimit is {} txnAutoRetry is {}", txnRetryLimit, isDisableTxnRetry());
        RuntimeException conflictException = exception;
        while (isDisableTxnRetry() && (txnRetryLimit-- > 0) && !transaction.isPessimistic()) {
            ((DingoStatement) statement).removeJob(jobManager);
            DingoDriverParser parser = new DingoDriverParser((DingoConnection) connection);
            Signature signature1 = parser.retryQuery(jobManager, sh.toString(), sh.signature.sql,
                ((DingoSignature) sh.signature).getSqlNode(), ((DingoSignature) sh.signature).getRelNode(),
                ((DingoSignature) sh.signature).getParasType(),
                signature.columns);
            ((DingoStatement) statement).setSignature(signature1);
            resultSet.setIterator(null);
            Frame frame = getFrame(sh, offset, fetchMaxRowCount);
            return frame;
        }
        if (conflictException != null) {
            throw conflictException;
        }
        return null;
    }


    public int getTxnRetryLimit() throws SQLException {
        Optional<String> retryCountOpt = Optional.ofNullable(
            connection.getClientInfo("txn_retry_cnt"));
        return retryCountOpt
            .map(Integer::parseInt)
            .orElse(0);
    }

    public boolean isDisableTxnRetry() throws SQLException {
        return "on".equalsIgnoreCase(connection.getClientInfo("txn_retry"));
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
        @NonNull StatementHandle sh,
        List<TypedValue> parameterValues,
        int maxRowsInFirstFrame
    ) throws NoSuchStatementException {
        DingoPreparedStatement statement = (DingoPreparedStatement) ((DingoConnection) connection).getStatement(sh);
        // In a non-batch prepared statement call, the parameter values are already set, but we set here to make this
        // function reusable for batch call.
        statement.setParameterValues(parameterValues);
        ITransaction transaction = ((DingoConnection) connection).getTransaction();
        if (transaction == null) {
            transaction = prepareJobAndTxn(sh, statement);
        }
        try {
            if (statement.getStatementType().canUpdate()) {
                final Iterator<Object[]> iterator = createIterator(statement);
                MetaResultSet metaResultSet = MetaResultSet.count(
                    sh.connectionId,
                    sh.id,
                    ((Number) iterator.next()[0]).longValue()
                );
                if (iterator instanceof JobIteratorImpl) {
                    iterator.hasNext();
                    JobIteratorImpl jobIterator = (JobIteratorImpl) iterator;
                    boolean hasIncId;
                    Long autoIncId = null;
                    if (jobIterator.getAutoIncId() == null) {
                        hasIncId = false;
                    } else {
                        hasIncId = true;
                        autoIncId = jobIterator.getAutoIncId();;
                    }
                    statement.setHasIncId(hasIncId);
                    statement.setAutoIncId(autoIncId);
                }
                if (transaction != null) {
                    transaction.addSql(statement.getSql());
                    if (transaction.getType() == TransactionType.NONE || transaction.isAutoCommit()) {
                        try {
                            connection.commit();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                return new ExecuteResult(ImmutableList.of(metaResultSet));
            }
            MetaResultSet metaResultSet;
            final Frame frame = new Frame(0, false, Collections.emptyList());
            metaResultSet = MetaResultSet.create(sh.connectionId, sh.id, false, sh.signature, frame);
            if (transaction != null) {
                transaction.addSql(statement.getSql());
                if (transaction.getType() == TransactionType.NONE || transaction.isAutoCommit()) {
                    try {
                        connection.commit();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            return new ExecuteResult(ImmutableList.of(metaResultSet));
        } catch (Throwable throwable) {
            log.error("run job exception:{}", throwable, throwable);
            if (transaction != null && transaction.isPessimistic()
                && transaction.getPrimaryKeyLock() != null
                && statement.isDml()) {
                // rollback pessimistic lock
                transaction.rollBackPessimisticLock(jobManager);
            }
            if (transaction != null) {
                transaction.addSql(statement.getSql());
                if (transaction.getType() == TransactionType.NONE || transaction.isAutoCommit()) {
                    try {
                        cleanTransaction();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            throw ExceptionUtils.toRuntime(throwable);
        }
    }

    private ITransaction prepareJobAndTxn(@NonNull StatementHandle sh, DingoPreparedStatement statement) {
        ITransaction transaction = null;
        Job job = statement.getJob(jobManager);
        if (job != null) {
            Task task = job.getTasks().entrySet().stream().findFirst().get().getValue();
            if (task.getTransactionType() == TransactionType.OPTIMISTIC
                || task.getTransactionType() == TransactionType.NONE) {
                transaction = ((DingoConnection) connection).createTransaction(
                    task.getTransactionType(),
                    true
                );
                statement.setTxnId(jobManager, transaction.getTxnId());
            } else {
                jobManager.removeJob(statement.getJobId(jobManager));
                DingoConnection dingoConnection = (DingoConnection) connection;
                DingoDriverParser parser = new DingoDriverParser(dingoConnection);
                sh.signature = parser.parseQuery(jobManager, sh.toString(), statement.getSql());
            }
        }
        return transaction;
    }

    public ExecuteResult execBatch(
        @NonNull StatementHandle sh,
        List<TypedValue> parameterValues,
        int maxRowsInFirstFrame
    ) throws NoSuchStatementException {
        DingoPreparedStatement statement = (DingoPreparedStatement) ((DingoConnection) connection).getStatement(sh);
        // In a non-batch prepared statement call, the parameter values are already set, but we set here to make this
        // function reusable for batch call.
        statement.setParameterValues(parameterValues);
        if (statement.getStatementType().canUpdate()) {
            final Iterator<Object[]> iterator = createIterator(statement);
            MetaResultSet metaResultSet = MetaResultSet.count(
                sh.connectionId,
                sh.id,
                ((Number) iterator.next()[0]).longValue()
            );
            return new ExecuteResult(ImmutableList.of(metaResultSet));
        }
        MetaResultSet metaResultSet;
        final Frame frame = new Frame(0, false, Collections.emptyList());
        metaResultSet = MetaResultSet.create(sh.connectionId, sh.id, false, sh.signature, frame);
        return new ExecuteResult(ImmutableList.of(metaResultSet));
    }

    @Override
    public void closeStatement(@NonNull StatementHandle sh) {
        // Called in `AvaticaStatement.close` to do extra things.
        AvaticaStatement statement = connection.statementMap.get(sh.id);
        if (statement instanceof DingoStatement) {
            ((DingoStatement) statement).removeJob(jobManager);
        } else if (statement instanceof DingoPreparedStatement) {
            ((DingoPreparedStatement) statement).removeJob(jobManager);
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

    public synchronized void cleanTransaction() throws SQLException {
        try {
            ITransaction transaction = ((DingoConnection) connection).getTransaction();
            if (transaction != null) {
                transaction.close(jobManager);
            }
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            ((DingoConnection) connection).cleanTransaction();
        }
    }

    @Override
    @SneakyThrows
    public void commit(ConnectionHandle ch) {
        try {
            ITransaction transaction = ((DingoConnection) connection).getTransaction();
            if (transaction != null) {
                transaction.commit(jobManager);
            }
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            cleanTransaction();
        }
    }

    @Override
    @SneakyThrows
    public void rollback(ConnectionHandle ch) {
        try {
            ITransaction transaction = ((DingoConnection) connection).getTransaction();
            if (transaction != null) {
                transaction.rollback(jobManager);
            }
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            cleanTransaction();
        }
    }

    @Override
    public MetaResultSet getTables(
        ConnectionHandle ch,
        String catalog,
        @NonNull Pat schemaPattern,
        Pat tableNamePattern,
        List<String> typeList
    ) {
        final Collection<CalciteSchema.TableEntry> tables = getMatchedTables(
            getMatchedSubSchema(((DingoConnection) connection).getContext().getRootSchema(), schemaPattern),
            tableNamePattern
        );
        tables.removeIf(t ->
             t == null || t.schema == null || t.getTable() == null || t.getTable().getJdbcTableType() == null
        );
        return createArrayResultSet(
            Linq4j.asEnumerable(tables)
                .select(t -> {
                    String tableType;
                    if (t.getTable() instanceof DingoTable) {
                        DingoTable table = (DingoTable) t.getTable();
                        tableType = table.getTable().tableType;
                    } else {
                        tableType = t.getTable().getJdbcTableType().jdbcName;
                    }
                    return new Object[]{
                    catalog,
                    t.schema.name,
                    t.name,
                    tableType
                    };
                }),
            new Class[] {String.class, String.class, String.class, String.class},
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
        @NonNull Pat schemaPattern,
        Pat tableNamePattern,
        @NonNull Pat columnNamePattern
    ) {
        final DingoParserContext context = ((DingoConnection) connection).getContext();
        final Collection<CalciteSchema.TableEntry> tables = getMatchedTables(
            getMatchedSubSchema(context.getRootSchema(), schemaPattern), tableNamePattern
        );
        final Predicate<String> filter = patToFilter(columnNamePattern, true);
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
        return createArrayResultSet(
            Linq4j.asEnumerable(columns).select(column -> new Object[] {
                column.tableCat,
                column.tableSchem,
                column.tableName,
                column.columnName,
                column.dataType,
                column.typeName,
                column.columnSize,
                column.decimalDigits,
                column.numPrecRadix,
                column.nullable,
                column.charOctetLength,
                column.ordinalPosition,
                column.isNullable,
                column.isAutoincrement,
                column.isGeneratedcolumn
            }),
            new Class[] {String.class, String.class, String.class, String.class,
                Integer.class, String.class, Integer.class,
                String.class, Integer.class, Integer.class,
                Integer.class, Integer.class, String.class,
                String.class, String.class
            },
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
        final CalciteSchema rootSchema = ((DingoConnection) connection).getContext().getRootSchema();
        final Collection<CalciteSchema> schemas = getMatchedSubSchema(rootSchema, schemaPattern);
        return createArrayResultSet(
            Linq4j.asEnumerable(schemas)
                .select(schema -> new Object[]{schema.name, catalog}),
            new Class[] {String.class, String.class},
            "TABLE_SCHEM",
            "TABLE_CATALOG");
    }

    @Override
    public MetaResultSet getPrimaryKeys(ConnectionHandle ch, String catalog, String schemaName, String tableName) {
        final DingoConnection dingoConnection = (DingoConnection) connection;
        final DingoParserContext context = dingoConnection.getContext();
        final CalciteSchema rootSchema = context.getRootSchema();
        // todo: current version, ignore name case
        final CalciteSchema schema = rootSchema.getSubSchema(schemaName, false);
        final CalciteSchema.TableEntry table = schema.getTable(tableName, false);
        final Table tableDefinition = ((DingoTable) table.getTable()).getTable();
        final TupleMapping mapping = tableDefinition.keyMapping();
        return createArrayResultSet(
            Linq4j.asEnumerable(IntStream.range(0, mapping.size())
                .mapToObj(i -> {
                    Column c = tableDefinition.getColumns().get(mapping.get(i));
                    return new Object[] {
                        catalog,
                        schemaName,
                        tableName,
                        c.getName(),
                        (short) (i + 1)
                    };
                })
                .collect(Collectors.toList())
            ),
            new Class[]{String.class, String.class, String.class, String.class, Short.class},
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "COLUMN_NAME",
            "KEY_SEQ"
        );
    }

    @Override
    public MetaResultSet getIndexInfo(ConnectionHandle ch, String catalog, String schemaName,
                                      String tableName, boolean unique, boolean approximate) {
        final DingoConnection dingoConnection = (DingoConnection) connection;
        final DingoParserContext context = dingoConnection.getContext();
        final CalciteSchema rootSchema = context.getRootSchema();
        List<IndexScan> indexScanList = new ArrayList();
        if (schemaName != null) {
            // todo: current version, ignore name case
            final CalciteSchema calciteSchema = rootSchema.getSubSchema(schemaName, false);
            if (calciteSchema == null) {
                throw ExceptionUtils.toRuntime(new IllegalArgumentException("schema does not exist"));
            }
            DingoSchema schema  = (DingoSchema) calciteSchema.schema;
            // TODO scan index for {{schema}}
        }

        return createArrayResultSet(
            Linq4j.asEnumerable(indexScanList).select(indexScan -> new Object[] {
                indexScan.tableCat,
                indexScan.tableSchema,
                indexScan.tableName,
                indexScan.unique,
                indexScan.indexQualifier,
                indexScan.indexName,
                indexScan.type,
                indexScan.ordinalPosition,
                indexScan.columnName,
                indexScan.ascOrDesc,
                indexScan.cardinality,
                indexScan.pages,
                indexScan.filterCondition
            }),
            new Class[] {String.class, String.class, String.class, Boolean.class,
                String.class, String.class, Short.class,
                Short.class, String.class, String.class,
                String.class, Integer.class, String.class
            },
            "TABLE_CAT",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "UNIQUE",
            "INDEX_QUALIFIER",
            "INDEX_NAME",
            "TYPE",
            "ORDINAL_POSITION",
            "COLUMN_NAME",
            "ASC_OR_DESC",
            "CARDINALITY",
            "PAGES",
            "FILTER_CONDITION"
        );
    }

    @Override
    public ConnectionProperties connectionSync(ConnectionHandle ch, ConnectionProperties connProps) {
        if (connProps.isEmpty()) {
            return connProps;
        }
        try {
            if (connProps.isAutoCommit() != null) {
                String autoCommit;
                if (connProps.isAutoCommit()) {
                    autoCommit = "on";
                } else {
                    autoCommit = "off";
                }
                String serverAutoCommit = connection.getClientInfo("autocommit");
                if (!autoCommit.equalsIgnoreCase(serverAutoCommit)) {
                    connection.setClientInfo("autocommit", autoCommit);
                }
            }
            if (connProps.isReadOnly() != null) {
                String readOnly;
                if (connProps.isReadOnly()) {
                    readOnly = "on";
                } else {
                    readOnly = "off";
                }
                String serverReadOnly = connection.getClientInfo("transaction_read_only");
                if (!readOnly.equalsIgnoreCase(serverReadOnly)) {
                    connection.setClientInfo("transaction_read_only", readOnly);
                }
            }
            if (connProps.getTransactionIsolation() != null) {
                String txIsolation = null;
                if (connProps.getTransactionIsolation() == 1) {
                    txIsolation = "READ-UNCOMMITTED";
                } else if (connProps.getTransactionIsolation() == 2) {
                    txIsolation = "READ-COMMITTED";
                } else if (connProps.getTransactionIsolation() == 4) {
                    txIsolation = "REPEATABLE-READ";
                } else if (connProps.getTransactionIsolation() == 8) {
                    txIsolation = "SERIALIZABLE";
                }
                if (txIsolation != null) {
                    String serverTxIsolation = connection.getClientInfo("transaction_isolation");
                    if (serverTxIsolation != null && !serverTxIsolation.equalsIgnoreCase(txIsolation)) {
                        connection.setClientInfo("transaction_isolation", txIsolation);
                    }
                }
            }
            if (connProps.getSchema() != null) {
                DingoConnection dingoConnection = (DingoConnection) connection;
                String usedSchema = dingoConnection.getContext().getUsedSchema().getName();
                if (!connProps.getSchema().equals(usedSchema)) {
                    CalciteSchema schema = dingoConnection.getContext()
                        .getRootSchema().getSubSchema(connProps.getSchema(), false);
                    if (schema != null) {
                        dingoConnection.getContext().setUsedSchema(schema);
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return connProps;
    }
}
