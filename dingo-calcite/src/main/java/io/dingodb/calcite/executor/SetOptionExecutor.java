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

package io.dingodb.calcite.executor;

import io.dingodb.calcite.grammar.ddl.SqlSetCharsetCollation;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.mysql.MysqlServer;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSetOption;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Map;
import java.util.Locale;

import static io.dingodb.common.mysql.scope.ScopeVariables.metricReporter;

@Slf4j
public class SetOptionExecutor implements DdlExecutor {

    public static final String CONNECTION_CHARSET = "character_set_connection";
    private static final String CLIENT_CHARSET = "character_set_client";
    public static final String RESULTS_CHARSET = "character_set_results";
    public static final String COLLATION_CONNECTION = "collation_connection";

    public Connection connection;

    private String scope;
    SqlSetOption sqlSetOption;

    private String name;

    private String value = "";

    public SetOptionExecutor(Connection connection, SqlSetOption setOption) {
        this.connection = connection;
        this.sqlSetOption = setOption;
        this.scope = setOption.getScope() == null ? "GLOBAL" : setOption.getScope().toUpperCase();
        SqlIdentifier sqlIdentifier = setOption.getName();
        if (sqlIdentifier.names.size() == 1) {
            name = sqlIdentifier.names.get(0);
        } else {
            name = sqlIdentifier.names.get(1);
        }
        if (name.equalsIgnoreCase("names")) {
            scope = "SESSION";
        }
        if ("USER".equals(scope)) {
            name = "@" + name;
        }
        name = name.toLowerCase();
        SqlNode sqlNode = setOption.getValue();
        if (sqlNode instanceof SqlNumericLiteral) {
            SqlNumericLiteral numericLiteral = (SqlNumericLiteral) sqlNode;
            value = Objects.requireNonNull(numericLiteral.getValue()).toString();
        } else if (sqlNode instanceof SqlIdentifier) {
            sqlIdentifier = (SqlIdentifier) sqlNode;
            if (name.equals("time_zone")) {
                value = sqlIdentifier.names.get(0);
            } else {
                value = sqlIdentifier.names.get(0).toLowerCase();
            }
        } else if (sqlNode instanceof SqlLiteral) {
            Object val = ((SqlLiteral) sqlNode).getValue();
            if (val != null) {
                value = val.toString();
            }
        } else if (sqlNode instanceof SqlCall) {
            value = evalSetExpression((SqlCall) sqlNode);
        }
        value = SqlParserUtil.trim(value, "'");
    }

    /**
     * Evaluate expression values in SET statements, e.g. MySQL tools send
     * {@code SET sql_mode = concat(@@sql_mode, ',STRICT_TRANS_TABLES')}.
     * Supported shapes: @@var references and CONCAT of literals/@@vars.
     */
    private String evalSetExpression(@NonNull SqlCall call) {
        String opName = call.getOperator().getName();
        if (opName.equals("@@")) {
            SqlNode variableNode = call.getOperandList().get(0);
            String variableName = variableNode.toString().replace("'", "").toLowerCase(Locale.ROOT);
            boolean global = variableName.startsWith("global.");
            if (global) {
                variableName = variableName.substring(7);
            } else if (variableName.startsWith("session.")) {
                variableName = variableName.substring(8);
            }
            try {
                String variableValue = global ? null : connection.getClientInfo(variableName);
                if (variableValue == null) {
                    Map<String, String> globalVariables = InfoSchemaService.root().getGlobalVariables();
                    variableValue = globalVariables.getOrDefault(variableName, "");
                }
                return variableValue;
            } catch (SQLException e) {
                throw new IllegalStateException("Cannot resolve SET variable reference: " + variableNode, e);
            }
        } else if (opName.equalsIgnoreCase("CONCAT")) {
            StringBuilder builder = new StringBuilder();
            for (SqlNode operand : call.getOperandList()) {
                builder.append(evalSetOperand(operand));
            }
            return builder.toString();
        }
        throw new IllegalArgumentException("Unsupported SET expression: " + call);
    }

    private String evalSetOperand(@NonNull SqlNode node) {
        if (node instanceof SqlCall) {
            return evalSetExpression((SqlCall) node);
        }
        if (node instanceof SqlLiteral) {
            String value = ((SqlLiteral) node).toValue();
            if (value != null) {
                return value;
            }
            throw new IllegalArgumentException("NULL is not supported in a SET CONCAT expression");
        }
        throw new IllegalArgumentException("Unsupported SET operand: " + node);
    }

    @Override
    public void execute() {
        try {
            value = VariableValidator.validator(name, value, scope);
            // optimistic transaction only support REPEATABLE-READ transaction isolation
            if ("transaction_isolation".equalsIgnoreCase(name)
                && "READ-COMMITTED".equalsIgnoreCase(value)
                && connection.getClientInfo("txn_mode").equalsIgnoreCase("optimistic")
            ) {
                return;
            }
            if ("SESSION".equals(scope) || "USER".equals(scope)) {
                if (!setCharacter(name, value)) {
                    connection.setClientInfo(name, value);
                }
            } else if ("SYSTEM".equals(scope)) {
                if ("time_zone".equals(name)) {
                    connection.setClientInfo(name, value);
                }
                try {
                    putGlobalVariable(name, value);
                } catch (WriteConflictException e) {
                    LogUtils.error(log, e.getMessage(), e);
                }
            } else if ("EXECUTOR".equals(scope)) {
                ScopeVariables.setExecutorProp(name, value);
            } else {
                if (name.equals("transaction_isolation")) {
                    connection.setClientInfo("onetime_transaction_isolation", value);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean setCharacter(String name, String value) {
        if (name.equalsIgnoreCase("names")) {
            value = value.toLowerCase();
            // todo Unknown encoding may cause connection failure
            // The known character sets include utf8, gbk, latin1, utf8mb4
            //if (!ScopeVariables.characterSet.contains(value)) {
            //    throw DINGO_RESOURCE.unknownCharacterSet(value).ex();
            //}
            try {
                connection.setClientInfo(CONNECTION_CHARSET, value);
                connection.setClientInfo(CLIENT_CHARSET, value);
                connection.setClientInfo(RESULTS_CHARSET, value);
            } catch (SQLClientInfoException e) {
                throw new RuntimeException(e);
            }
            if (sqlSetOption instanceof SqlSetCharsetCollation) {
                SqlSetCharsetCollation sqlSetCharsetCollation = (SqlSetCharsetCollation) sqlSetOption;
                String collation = sqlSetCharsetCollation.collation;
                try {
                    connection.setClientInfo(COLLATION_CONNECTION, collation);
                } catch (SQLClientInfoException e) {
                    throw new RuntimeException(e);
                }
            }
            return true;
        }
        return false;
    }

    public static void putGlobalVariable(String key, String value) {
        if (StringUtils.isBlank(key)) {
            return;
        }
        if ("metric_log_enable".equalsIgnoreCase(key)) {
            if (value == null) {
                return;
            }
            metricReporter(value);
        }
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        infoSchemaService.putGlobalVariable(key, value);
        if ("SSL_ENABLE".equalsIgnoreCase(key)) {
            MysqlServer.SSL_ENABLE = "on".equalsIgnoreCase(value);
        }
    }

}
