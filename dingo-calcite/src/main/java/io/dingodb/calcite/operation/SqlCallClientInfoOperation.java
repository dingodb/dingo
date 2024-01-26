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

import io.dingodb.calcite.grammar.ddl.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;

import java.sql.Connection;
import java.sql.SQLException;

public class SqlCallClientInfoOperation implements DdlOperation {
    private final Connection connection;

    private final SqlCall sqlCall;
    public SqlCallClientInfoOperation(Connection connection, SqlCall call) {
        this.connection = connection;
        this.sqlCall = call;
    }

    @Override
    public void execute() {
        SqlIdentifier sqlIdentifier = sqlCall.getCall();
        if (sqlIdentifier.names.size() == 2
            && sqlIdentifier.names.get(1).equalsIgnoreCase("setClientInfo")) {
            if (sqlCall.getVal().length == 2) {
                String key = (String) sqlCall.getVal()[0];
                try {
                    if (connection.getClientInfo(key) != null) {
                        connection.setClientInfo(key, (String) sqlCall.getVal()[1]);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
