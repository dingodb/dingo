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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QueryVariableOperation implements QueryOperation {
    String variableName;

    boolean isSession;
    Connection connection;

    public QueryVariableOperation(SqlBasicCall sqlBasicCall, Connection connection) {
        SqlNode sqlNode = sqlBasicCall.getOperandList().get(0);
        this.variableName = sqlNode.toString().replace("'", "").toLowerCase();
        if (variableName.startsWith("session.")) {
            variableName = variableName.substring(8);
            isSession = true;
        }
        this.connection = connection;
    }

    @Override
    public Iterator getIterator() {
        try {
            List<Object[]> variableValList = new ArrayList<>();
            Object value;
            if (isSession) {
                value = connection.getClientInfo().getOrDefault(variableName, "");
            } else {
                value = ScopeVariables.globalVariables.getOrDefault(variableName, "");
            }
            value = transform(variableName, value);
            variableValList.add(new Object[]{value});
            return variableValList.iterator();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Object transform(String variableName, Object value) {
        String val = value.toString();
        if (val.equalsIgnoreCase("OFF")) {
            return "0";
        } else if (val.equalsIgnoreCase("ON")) {
            return "1";
        } else {
            return value;
        }
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        StringBuilder colName = new StringBuilder("@@");
        if (isSession) {
            colName.append("session.");
        }
        colName.append(variableName);
        columns.add(colName.toString());
        return columns;
    }
}
