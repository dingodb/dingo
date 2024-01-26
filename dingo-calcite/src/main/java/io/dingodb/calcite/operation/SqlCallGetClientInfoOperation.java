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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SqlCallGetClientInfoOperation implements QueryOperation {
    private final Connection connection;
    private String key = "";

    public SqlCallGetClientInfoOperation(Connection connection, SqlCall sqlCall) {
        this.connection = connection;
        if (sqlCall.getVal() != null && sqlCall.getVal().length > 0) {
            key = (String) sqlCall.getVal()[0];
        }
    }

    @Override
    public Iterator<Object[]> getIterator() {
        List<Object[]> resList = new ArrayList<>();
        try {
            resList.add(new Object[] {connection.getClientInfo(key)});
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return resList.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add(key);
        return columns;
    }
}
