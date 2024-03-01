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
import io.dingodb.calcite.grammar.dql.SqlShowCall;
import io.dingodb.common.mysql.scope.ScopeVariables;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class ShowCallsOperation implements QueryOperation {

    private final List<SqlShowCall> sqlShowCallList;

    private final Connection connection;
    private final DingoParserContext context;

    public ShowCallsOperation(List<SqlShowCall> sqlShowCallList, Connection connection, DingoParserContext context) {
        this.sqlShowCallList = sqlShowCallList;
        this.connection = connection;
        this.context = context;
    }

    @Override
    public Iterator<Object[]> getIterator() {
        List<Object[]> tupleList = new ArrayList<>();
        tupleList.add(sqlShowCallList.stream().map(call -> {
            if (call.getCall().getOperator().getName().equals("@")) {
                SqlNode sqlNode = call.getCall().getOperandList().get(0);
                String variableName = "@" + sqlNode.toString().replace("'", "");
                try {
                    return connection.getClientInfo(variableName);
                } catch (SQLException e) {
                    return "";
                }
            } else if (call.getCall().getOperator().getName().equals("@@")) {
                SqlNode sqlNode = call.getCall().getOperandList().get(0);
                String variableName = sqlNode.toString().replace("'", "").toLowerCase();
                String value = null;

                if (variableName.startsWith("global.")) {
                    variableName = variableName.substring(7);
                    value = ScopeVariables.globalVariables.getOrDefault(variableName, "").toString();
                } else if (variableName.startsWith("session.")) {
                    variableName = variableName.substring(8);
                    try {
                        if ("tx_isolation".equals(variableName)) {
                            variableName = "transaction_isolation";
                        } else if ("tx_read_only".equals(variableName)) {
                            variableName = "transaction_read_only";
                        }
                        value = connection.getClientInfo(variableName);
                    } catch (SQLException e) {
                        log.error(e.getMessage(), e);
                    }
                } else {
                    try {
                        value = connection.getClientInfo(variableName);
                    } catch (SQLException e) {
                        log.error(e.getMessage(), e);
                    }
                }
                if (value == null) {
                    value = "";
                }
                if (value.equalsIgnoreCase("on")) {
                    return 1;
                } else if (value.equalsIgnoreCase("off")) {
                    return 0;
                } else {
                    return value;
                }
            } else if (call.getCall().getOperator().getName().equalsIgnoreCase("database")) {
                if (context.getUsedSchema() != null) {
                    return context.getUsedSchema().getName();
                } else {
                    return "dingo";
                }
            } else {
                return "";
            }
        }).toArray());
        return tupleList.iterator();
    }

    @Override
    public List<String> columns() {
        return sqlShowCallList.stream().map(SqlShowCall::getName).collect(Collectors.toList());
    }
}
