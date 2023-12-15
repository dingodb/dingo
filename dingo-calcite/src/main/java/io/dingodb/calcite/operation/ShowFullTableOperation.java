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

import io.dingodb.common.util.SqlLikeUtils;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowFullTableOperation implements QueryOperation {
    private String schemaName;
    private String sqlLikePattern;
    private SqlNode condition;
    private Connection connection;

    public ShowFullTableOperation(String schemaName,
                                  String pattern,
                                  Connection connection,
                                  SqlNode condition) {
        this.schemaName = schemaName;
        this.sqlLikePattern = pattern;
        this.connection = connection;
        this.condition = condition;
    }

    @Override
    public Iterator getIterator() {
        try {
            List<Object[]> tables = new ArrayList<>();
            ResultSet rs = connection.getMetaData().getTables(null, schemaName.toUpperCase(),
                null, null);
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                String tableType = rs.getString("TABLE_TYPE");
                if (StringUtils.isBlank(sqlLikePattern) || SqlLikeUtils.like(tableName, sqlLikePattern)) {
                    tables.add(new Object[]{tableName.toLowerCase(), tableType});
                }
            }
            return tables.iterator();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("Tables_in_" + schemaName);
        columns.add("Table_type");
        return columns;
    }
}
