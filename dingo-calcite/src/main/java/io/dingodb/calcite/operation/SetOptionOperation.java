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

import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.meta.InfoSchemaService;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSetOption;

import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.Objects;

public class SetOptionOperation implements DdlOperation {

    public static final String CONNECTION_CHARSET = "character_set_connection";
    private static final String CLIENT_CHARSET = "character_set_client";
    private static final String RESULTS_CHARSET = "character_set_results";

    public Connection connection;

    private String scope;

    private String name;

    private String value = "";

    public SetOptionOperation(Connection connection, SqlSetOption setOption) {
        this.connection = connection;
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
            value = sqlIdentifier.names.get(0).toLowerCase();
        } else if (sqlNode instanceof SqlLiteral) {
            Object val = ((SqlLiteral) sqlNode).getValue();
            if (val != null) {
                value = val.toString();
            }
        }
        if (value.startsWith("'") && value.endsWith("'")) {
            value = value.substring(1, value.length() - 1);
        }
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
                throw new RuntimeException("Optimistic transaction mode cannot be changed" +
                    " to read committed transaction isolation level");
            }
            if ("SESSION".equals(scope) || "USER".equals(scope)) {
                if (!setCharacter(name, value)) {
                    connection.setClientInfo(name, value);
                }
            } else if ("SYSTEM".equals(scope)) {
                putGlobalVariable(name, value);
                ScopeVariables.globalVariables.put(name, value);
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
            return true;
        }
        return false;
    }

    public static void putGlobalVariable(String key, String value) {
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        infoSchemaService.putGlobalVariable(key, value);
    }

}
