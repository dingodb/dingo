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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowTableOperation implements Operation{
    private String schemaName;
    Connection connection;

    public ShowTableOperation(String schemaName, Connection connection) {
        this.schemaName = schemaName;
        this.connection = connection;
    }

    @Override
    public Iterator getIterator() {
        try {
            List<Object[]> tables = new ArrayList<>();
            ResultSet rs = connection.getMetaData().getTables(null, null, null, null);
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEM");
                if (schema.equalsIgnoreCase(schemaName)) {
                    Object[] tuples = new Object[] {rs.getString("TABLE_NAME")};
                    tables.add(tuples);
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
        return columns;
    }
}
