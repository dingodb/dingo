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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowLastInsertIdOperation implements QueryOperation {
    Connection connection;

    public ShowLastInsertIdOperation(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Iterator getIterator() {
        try {
            List<Object[]> variableValList = new ArrayList<>();
            String value = connection.getClientInfo().getProperty("last_insert_id", "0");
            variableValList.add(new Object[]{value});
            return variableValList.iterator();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("last_insert_id()");
        return columns;
    }
}
