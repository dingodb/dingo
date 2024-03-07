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

package io.dingodb.calcite.operation;

import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.grammar.ddl.SqlAnalyze;
import io.dingodb.calcite.grammar.ddl.SqlBeginTx;
import io.dingodb.calcite.grammar.ddl.SqlCall;
import io.dingodb.calcite.grammar.ddl.SqlCommit;
import io.dingodb.calcite.grammar.ddl.SqlKillConnection;
import io.dingodb.calcite.grammar.ddl.SqlKillQuery;
import io.dingodb.calcite.grammar.ddl.SqlLoadData;
import io.dingodb.calcite.grammar.ddl.SqlLockBlock;
import io.dingodb.calcite.grammar.ddl.SqlLockTable;
import io.dingodb.calcite.grammar.ddl.SqlRollback;
import io.dingodb.calcite.grammar.ddl.SqlUnLockBlock;
import io.dingodb.calcite.grammar.ddl.SqlUnLockTable;
import io.dingodb.calcite.grammar.dql.SqlDesc;
import io.dingodb.calcite.grammar.dql.SqlNextAutoIncrement;
import io.dingodb.calcite.grammar.dql.SqlShowCall;
import io.dingodb.calcite.grammar.dql.SqlShowCharset;
import io.dingodb.calcite.grammar.dql.SqlShowCollation;
import io.dingodb.calcite.grammar.dql.SqlShowColumns;
import io.dingodb.calcite.grammar.dql.SqlShowCreateTable;
import io.dingodb.calcite.grammar.dql.SqlShowCreateUser;
import io.dingodb.calcite.grammar.dql.SqlShowDatabases;
import io.dingodb.calcite.grammar.dql.SqlShowEngines;
import io.dingodb.calcite.grammar.dql.SqlShowFullTables;
import io.dingodb.calcite.grammar.dql.SqlShowGrants;
import io.dingodb.calcite.grammar.dql.SqlShowLocks;
import io.dingodb.calcite.grammar.dql.SqlShowPlugins;
import io.dingodb.calcite.grammar.dql.SqlShowProcessList;
import io.dingodb.calcite.grammar.dql.SqlShowTableDistribution;
import io.dingodb.calcite.grammar.dql.SqlShowTableStatus;
import io.dingodb.calcite.grammar.dql.SqlShowTables;
import io.dingodb.calcite.grammar.dql.SqlShowVariables;
import io.dingodb.calcite.grammar.dql.SqlShowWarnings;
import io.dingodb.exec.transaction.base.TransactionType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public final class SqlToOperationConverter {

    private SqlToOperationConverter() {
    }

    public static Optional<Operation> convert(SqlNode sqlNode, Connection connection, DingoParserContext context) {
        if (sqlNode instanceof SqlShowWarnings) {
            return Optional.of(new ShowWarningsOperation(context));
        } else if (sqlNode instanceof SqlShowGrants) {
            return Optional.of(new ShowGrantsOperation(sqlNode));
        } else if (sqlNode instanceof SqlShowDatabases) {
            SqlShowDatabases sqlShowDatabases = (SqlShowDatabases) sqlNode;
            return Optional.of(new ShowDatabaseOperation(connection, sqlShowDatabases.sqlLikePattern));
        } else if (sqlNode instanceof SqlShowTables) {
            String usedSchema = "";
            if (context.getUsedSchema() != null) {
                usedSchema = context.getUsedSchema().getName();
            }
            SqlShowTables sqlShowTables = (SqlShowTables) sqlNode;
            String pattern = sqlShowTables.sqlLikePattern;
            return Optional.of(new ShowTableOperation(usedSchema, connection, pattern));
        } else if (sqlNode instanceof SqlShowFullTables) {
            SqlShowFullTables showFullTables = (SqlShowFullTables) sqlNode;
            return Optional.of(
                new ShowFullTableOperation(
                    showFullTables.schema,
                    showFullTables.pattern,
                    connection,
                    showFullTables.condition)
            );
        } else if (sqlNode instanceof SqlNextAutoIncrement) {
            SqlNextAutoIncrement sqlNextAutoIncrement = (SqlNextAutoIncrement) sqlNode;
            if (StringUtils.isEmpty(sqlNextAutoIncrement.schemaName)) {
                sqlNextAutoIncrement.schemaName = getSchemaName(context);
            }
            return Optional.of(new ShowNextAutoIncrementOperation(sqlNextAutoIncrement));
        } else if (sqlNode instanceof SqlSelect) {
            SqlSelect sqlSelect = ((SqlSelect) sqlNode);
            List<SqlShowCall> sqlShowCallList = getSqlShowCallList(sqlSelect);
            return Optional.of(new ShowCallsOperation(sqlShowCallList, connection, context));
        } else if (sqlNode instanceof SqlOrderBy) {
            SqlSelect sqlSelect = (SqlSelect) ((SqlOrderBy) sqlNode).query;
            List<SqlShowCall> sqlShowCallList = getSqlShowCallList(sqlSelect);
            return Optional.of(new ShowCallsOperation(sqlShowCallList, connection, context));
        } else if (sqlNode instanceof SqlShowVariables) {
            SqlShowVariables sqlShowVariables = (SqlShowVariables) sqlNode;
            return Optional.of(new ShowVariablesOperation(sqlShowVariables.sqlLikePattern, sqlShowVariables.isGlobal,
                connection));
        } else if (sqlNode instanceof SqlSetOption) {
            SqlSetOption setOption = (SqlSetOption) sqlNode;
            return Optional.of(new SetOptionOperation(connection, setOption));
        } else if (sqlNode instanceof SqlShowCreateTable) {
            return Optional.of(new ShowCreateTableOperation(sqlNode, getSchemaName(context)));
        } else if (sqlNode instanceof SqlShowCreateUser) {
            SqlShowCreateUser sqlShowCreateUser = (SqlShowCreateUser) sqlNode;
            return Optional.of(new ShowCreateUserOperation(sqlNode,
                sqlShowCreateUser.userName,
                sqlShowCreateUser.host));
        } else if (sqlNode instanceof SqlShowColumns) {
            SqlShowColumns showColumns = (SqlShowColumns) sqlNode;
            if (StringUtils.isEmpty(showColumns.schemaName)) {
                showColumns.schemaName = getSchemaName(context);
            }
            return Optional.of(new ShowColumnsOperation(sqlNode));
        } else if (sqlNode instanceof SqlShowTableDistribution) {
            SqlShowTableDistribution sqlShowTableDistribution = (SqlShowTableDistribution) sqlNode;
            if (sqlShowTableDistribution.schemaName == null) {
                sqlShowTableDistribution.schemaName = getSchemaName(context);
            }
            return Optional.of(new ShowTableDistributionOperation(
                sqlNode, sqlShowTableDistribution.schemaName, sqlShowTableDistribution.tableName
            ));
        } else if (sqlNode instanceof SqlDesc) {
            SqlDesc sqlDesc = (SqlDesc) sqlNode;
            if (StringUtils.isEmpty(sqlDesc.schemaName)) {
                sqlDesc.schemaName = getSchemaName(context);
            }
            SqlShowColumns sqlShowColumns = new SqlShowColumns(sqlDesc.pos, sqlDesc.schemaName, sqlDesc.tableName, "");
            return Optional.of(new ShowColumnsOperation(sqlShowColumns));
        } else if (sqlNode instanceof SqlShowTableStatus) {
            SqlShowTableStatus showTableStatus = (SqlShowTableStatus) sqlNode;
            return Optional.of(new ShowTableStatusOperation(showTableStatus.schema, showTableStatus.sqlLikePattern));
        } else if (sqlNode instanceof SqlAnalyze) {
            SqlAnalyze analyze = (SqlAnalyze) sqlNode;
            if (StringUtils.isEmpty(analyze.getSchemaName())) {
                analyze.setSchemaName(getSchemaName(context));
            }
            return Optional.of(new AnalyzeTableOperation(analyze, connection));
        } else if (sqlNode instanceof SqlCommit) {
            return Optional.of(new CommitTxOperation(connection));
        } else if (sqlNode instanceof SqlRollback) {
            return Optional.of(new RollbackTxOperation(connection));
        } else if (sqlNode instanceof SqlBeginTx) {
            SqlBeginTx sqlBeginTx = (SqlBeginTx) sqlNode;
            boolean pessimistic = false;
            try {
                if (TransactionType.PESSIMISTIC.name().equalsIgnoreCase(sqlBeginTx.txnMode)) {
                    pessimistic = true;
                } else if (sqlBeginTx.txnMode.equals("")
                    && TransactionType.PESSIMISTIC.name().equalsIgnoreCase(connection.getClientInfo("txn_mode"))) {
                    pessimistic = true;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return Optional.of(new StartTransactionOperation(connection, pessimistic));
        } else if (sqlNode instanceof SqlLockTable) {
            SqlLockTable sqlLockTable = (SqlLockTable) sqlNode;
            String usedSchemaName = getSchemaName(context);
            return Optional.of(new LockTableOperation(connection, sqlLockTable.tableList, usedSchemaName));
        } else if (sqlNode instanceof SqlLockBlock) {
            SqlLockBlock sqlLockBlock = (SqlLockBlock) sqlNode;
            return Optional.of(new LockBlockOperation(connection, sqlLockBlock.getSqlBlockList()));
        } else if (sqlNode instanceof SqlUnLockTable) {
            return Optional.of(new UnlockTableOperation(connection));
        } else if (sqlNode instanceof SqlUnLockBlock) {
            return Optional.of(new UnlockBlockOperation(connection));
        } else if (sqlNode instanceof SqlKillQuery) {
            SqlKillQuery killQuery = (SqlKillQuery) sqlNode;
            return Optional.of(new KillQuery(killQuery.getThreadId()));
        } else if (sqlNode instanceof SqlKillConnection) {
            SqlKillConnection killConnection = (SqlKillConnection) sqlNode;
            return Optional.of(new KillConnection(killConnection.getThreadId()));
        } else if (sqlNode instanceof SqlShowEngines) {
            SqlShowEngines sqlShowEngines = (SqlShowEngines) sqlNode;
            return Optional.of(new ShowEnginesOperation(sqlShowEngines.sqlLikePattern));
        } else if (sqlNode instanceof SqlShowPlugins) {
            SqlShowPlugins sqlShowPlugins = (SqlShowPlugins) sqlNode;
            return Optional.of(new ShowPluginsOperation(sqlShowPlugins.sqlLikePattern));
        } else if (sqlNode instanceof SqlShowCollation) {
            SqlShowCollation sqlShowCollation = (SqlShowCollation) sqlNode;
            return Optional.of(new ShowCollationOperation(sqlShowCollation.sqlLikePattern));
        } else if (sqlNode instanceof SqlShowCharset) {
            SqlShowCharset sqlShowCharset = (SqlShowCharset) sqlNode;
            return Optional.of(new ShowCharsetOperation(sqlShowCharset.sqlLikePattern));
        } else if (sqlNode instanceof SqlShowLocks) {
            return Optional.of(new ShowLocksOperation(
                ((SqlShowLocks) sqlNode).filterIdentifier,
                ((SqlShowLocks) sqlNode).filterKind,
                ((SqlShowLocks) sqlNode).filterOperand)
            );
        } else if (sqlNode instanceof SqlCall) {
            SqlCall sqlCall = (SqlCall) sqlNode;
            if (sqlCall.getCall().names.size() == 2) {
                String operation = sqlCall.getCall().names.get(1);
                if (operation.equalsIgnoreCase("getClientInfo")) {
                    return Optional.of(new SqlCallGetClientInfoOperation(connection, sqlCall));
                } else if (operation.equalsIgnoreCase("setClientInfo")) {
                    return Optional.of(new SqlCallClientInfoOperation(connection, (SqlCall) sqlNode));
                }
            }
            return Optional.empty();
        } else if (sqlNode instanceof SqlLoadData) {
            SqlLoadData sqlLoadData = (SqlLoadData) sqlNode;
            if (StringUtils.isBlank(sqlLoadData.getSchemaName())) {
                sqlLoadData.setSchemaName(getSchemaName(context));
            }
            return Optional.of(new LoadDataOperation(sqlLoadData, connection, context));
        } else if (sqlNode instanceof SqlShowProcessList) {
            SqlShowProcessList showProcessList = (SqlShowProcessList) sqlNode;
            String user = context.getOption("user");
            String host = context.getOption("host");
            return Optional.of(new ShowProcessListOperation(showProcessList.isProcessPrivilege(), user, host));
        } else {
            return Optional.empty();
        }
    }

    private static List<SqlShowCall> getSqlShowCallList(SqlSelect sqlSelect) {
        SqlNodeList sqlNodes = sqlSelect.getSelectList();
        return sqlNodes.stream().map(e -> {
            SqlBasicCall call = (SqlBasicCall) e;
            String opName = call.getOperator().getName();
            if (opName.equalsIgnoreCase("AS")) {
                SqlIdentifier identifier = (SqlIdentifier) call.getOperandList().get(1);
                opName = identifier.getSimple();
                call = (SqlBasicCall) call.getOperandList().get(0);
            } else {
                StringBuilder colNmStr = new StringBuilder(opName);
                if (call.getOperandList() != null && call.getOperandList().size() > 0) {
                    colNmStr.append(call.getOperandList().get(0).toString());
                }
                opName = colNmStr.toString();
                if (opName.contains("'")) {
                    opName = opName.replace("'", "");
                }
            }
            return new SqlShowCall(opName, call);
        }).collect(Collectors.toList());
    }

    private static String getSchemaName(DingoParserContext context) {
        if (context.getUsedSchema() == null) {
            return context.getDefaultSchemaName();
        }
        return context.getUsedSchema().getName();
    }

}
