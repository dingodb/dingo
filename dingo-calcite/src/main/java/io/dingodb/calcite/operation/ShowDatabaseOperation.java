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
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowDatabaseOperation implements QueryOperation {

    Connection connection;

    private String sqlLikePattern;

    public ShowDatabaseOperation(Connection connection, String sqlLikePattern) {
        this.connection = connection;
        this.sqlLikePattern = sqlLikePattern;
    }

    @Override
    public Iterator getIterator() {
        List<Object[]> schemas = new ArrayList<>();
        try {
            ResultSet rs = connection.getMetaData().getSchemas();
            while (rs.next()) {
                String schemaName = rs.getString("TABLE_SCHEM");
                if (schemaName.equalsIgnoreCase("ROOT")) {
                    continue;
                }
                if (StringUtils.isBlank(sqlLikePattern) || SqlLikeUtils.like(schemaName, sqlLikePattern)) {
                    schemas.add(new Object[] {schemaName.toLowerCase()});
                }
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
