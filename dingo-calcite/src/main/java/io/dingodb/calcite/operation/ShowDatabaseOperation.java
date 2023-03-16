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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.Meta;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowDatabaseOperation implements Operation {

    Connection connection;

    public ShowDatabaseOperation(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Iterator getIterator() {
        List<Object[]> schemas = new ArrayList<>();
        try {
            ResultSet rs = connection.getMetaData().getSchemas();
            while (rs.next()) {
                Object[] tuples = new Object[] {rs.getString("TABLE_SCHEM")};
                schemas.add(tuples);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return schemas.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns =  new ArrayList<>();
        columns.add("TABLE_SCHEM");
        return columns;
    }
}
