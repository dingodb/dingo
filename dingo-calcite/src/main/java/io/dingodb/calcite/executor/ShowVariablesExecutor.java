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

package io.dingodb.calcite.executor;

import io.dingodb.common.util.SqlLikeUtils;
import io.dingodb.meta.InfoSchemaService;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ShowVariablesExecutor extends QueryExecutor {

    private final String sqlLikePattern;

    private final boolean isGlobal;

    private final Connection connection;

    public ShowVariablesExecutor(String sqlLikePattern, boolean isGlobal, Connection connection) {
        this.sqlLikePattern = sqlLikePattern;
        this.isGlobal = isGlobal;
        this.connection = connection;
    }

    @Override
    public Iterator<Object[]> getIterator() {
        Properties variables;
        if (isGlobal) {
            InfoSchemaService infoSchemaService = InfoSchemaService.root();
            Map<String, String> globalVariableMap = infoSchemaService.getGlobalVariables();
            variables = new Properties();
            variables.putAll(globalVariableMap);
        } else {
            try {
                variables = connection.getClientInfo();
                if (variables.isEmpty()) {
                    InfoSchemaService infoSchemaService = InfoSchemaService.root();
                    Map<String, String> globalVariableMap = infoSchemaService.getGlobalVariables();
                    variables = new Properties();
                    variables.putAll(globalVariableMap);
                    connection.setClientInfo(new Properties());
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        List<Object[]> tables = new ArrayList<>();
        variables.forEach((key, value) -> {
            if ((StringUtils.isBlank(sqlLikePattern) || SqlLikeUtils.like(key.toString(), sqlLikePattern))
                && !key.toString().startsWith("@")) {
                tables.add(new Object[] {key,value});
            }
        });
        return tables.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("Variable_name");
        columns.add("Value");
        return columns;
    }
}
