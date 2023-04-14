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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSetOption;

import java.sql.Connection;
import java.sql.SQLClientInfoException;

public class SetOptionOperation implements DdlOperation {

    public Connection connection;

    private String name;

    private Object value;

    public SetOptionOperation(Connection connection, SqlSetOption setOption) {
        this.connection = connection;
        SqlIdentifier sqlIdentifier = setOption.getName();
        if (sqlIdentifier.names.size() == 1) {
            name = sqlIdentifier.names.get(0);
        } else {
            name = sqlIdentifier.names.get(1);
        }
        SqlNode sqlNode = setOption.getValue();
        if (sqlNode instanceof SqlNumericLiteral) {
            SqlNumericLiteral numericLiteral = (SqlNumericLiteral) sqlNode;
            value = numericLiteral.getValue();
        } else if (sqlNode instanceof SqlIdentifier) {
            sqlIdentifier = (SqlIdentifier) sqlNode;
            value = sqlIdentifier.names.get(0);
        } else if (sqlNode instanceof SqlLiteral) {
            value = "";
        }
    }

    @Override
    public void execute() {
        try {
            connection.setClientInfo(name, value.toString());
        } catch (SQLClientInfoException e) {
            throw new RuntimeException(e);
        }
    }
}