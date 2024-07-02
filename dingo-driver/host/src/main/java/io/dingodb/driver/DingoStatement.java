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
import io.dingodb.calcite.operation.DmlOperation;
import io.dingodb.calcite.operation.Operation;
import io.dingodb.calcite.operation.QueryOperation;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.profile.SqlProfile;
import io.dingodb.common.mysql.constant.ServerStatus;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.exception.TaskCancelException;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Iterator;

@Slf4j
public class DingoStatement extends AvaticaStatement {

    // for mysql protocol
    @Setter
    private boolean inTransaction;

    // for mysql protocol
    @Setter
    private boolean autoCommit;

    // for mysql protocol
    @Setter
    private boolean transReadOnly;
    private String warning;

    @Setter
    @Getter
    private boolean hasIncId;

    @Getter
    @Setter
    private SqlProfile sqlProfile;

    @Setter
    @Getter
    private Long autoIncId;

    @Getter
    @Setter
    private Job job;

    @Getter
    @Setter
    private JobManager jobManager;

    DingoStatement(
        DingoConnection connection,
        Meta.StatementHandle handle,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) {
        super(
            connection,
            handle,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability
        );
    }

    @Override
    protected void setSignature(Meta.Signature signature) {
        super.setSignature(signature);
    }

    @Override
    protected void executeInternal(String sql) throws SQLException {
        this.updateCount = -1;
        try {
            // In JDBC, maxRowCount = 0 means no limit; in prepare it means LIMIT 0
            final long maxRowCount1 = maxRowCount <= 0 ? -1 : maxRowCount;
            ((DingoConnection) connection).prepareAndExecuteInternal(this, sql, maxRowCount1);
        } catch (Throwable e) {
            throw ExceptionUtils.toSql(e);
        }
    }

    public void createResultSet(Meta.@Nullable Frame firstFrame) throws SQLException {
        if (openResultSet != null) {
            openResultSet.close();
        }
        Meta.Signature signature = getSignature();
        openResultSet = ((DingoConnection) connection).newResultSet(
            this,
            signature,
            firstFrame,
            signature.sql
        );
    }

    @Override
    public synchronized void close() throws SQLException {
        try {
            cancel();
        } finally {
            super.close();
        }
    }

    @Override
    public synchronized void cancel() throws SQLException {
        LogUtils.info(log, "dingo statement cancel," + this.handle.id);
        super.cancel();
        if (jobManager != null && job != null) {
            LogUtils.info(log, "dingo statement cancel job:{}", job);
            jobManager.cancel(job.getJobId());
        }
    }

    @NonNull
    @SneakyThrows
    public Iterator<Object[]> createIterator(@NonNull JobManager jobManager) {
        Meta.Signature signature = getSignature();
        if (signature instanceof DingoExplainSignature) {
            DingoExplainSignature explainSignature = (DingoExplainSignature) signature;
            return ImmutableList.of(new Object[]{explainSignature.toString()}).iterator();
        } else if (signature instanceof ExplainSignature) {
            ExplainSignature explainSignature = (ExplainSignature) signature;
            return explainSignature.getIterator();
        } else if (signature instanceof DingoSignature) {
            if (cancelFlag.get()) {
                throw new TaskCancelException("task is cancel");
            }
            Job job = jobManager.getJob(((DingoSignature) signature).getJobId());
            this.job = job;
            this.jobManager = jobManager;
            Iterator<Object[]> iterator = jobManager.createIterator(
                job, null, Optional.mapOrGet(connection.getClientInfo("max_execution_time"), Long::parseLong, () -> 0L)
            );
            if (cancelFlag.get()) {
                throw new TaskCancelException("task is cancel");
            }
            return iterator;
        } else if (signature instanceof MysqlSignature) {
            Operation operation = ((MysqlSignature) signature).getOperation();
            if (operation instanceof QueryOperation) {
                Iterator<Object[]> iterator = ((QueryOperation) operation).iterator();
                if (cancelFlag.get()) {
                    LogUtils.info(log, "cancelFlag is true");
                    throw new SQLException("Statement canceled");
                }
                return iterator;
            } else {
                DmlOperation dmlOperation = (DmlOperation) operation;
                Iterator<Object[]> iterator = dmlOperation.getIterator();
                warning = dmlOperation.getWarning();
                if (cancelFlag.get()) {
                    LogUtils.info(log, "cancelFlag is true");
                    throw new SQLException("Statement canceled");
                }
                return iterator;
            }
        }
        throw ExceptionUtils.wrongSignatureType(this, signature);
    }

    public void removeJob(JobManager jobManager) {
        Meta.Signature signature = getSignature();
        DingoStatementUtils.removeJobInSignature(jobManager, signature);
    }

    @Override
    public SQLWarning getWarnings() {
        if (warning == null) {
            return null;
        } else {
            return new SQLWarning(warning);
        }
    }

    public int getServerStatus() {
        int initServerStatus = 0;
        if (inTransaction) {
            initServerStatus = ServerStatus.SERVER_STATUS_IN_TRANS;
        }
        if (autoCommit) {
            initServerStatus |= ServerStatus.SERVER_STATUS_AUTOCOMMIT;
        }
        if (transReadOnly) {
            initServerStatus |= ServerStatus.SERVER_STATUS_IN_TRANS_READONLY;
        }
        return initServerStatus;
    }

    public void initSqlProfile() {
        sqlProfile = new SqlProfile("statement", false);
        DingoConnection connection1 = (DingoConnection) connection;
        sqlProfile.setSchema(connection1.getContext().getUsedSchema().getName());
        String user = connection1.getContext().getOption("user");
        String host = connection1.getContext().getOption("host");
        sqlProfile.setSimpleUser(user + "@" + host);
        sqlProfile.setInstance(DingoConfiguration.location().toString());
    }

}
