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

package io.dingodb.test.dsl.run.exec;

import io.dingodb.test.utils.TableUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public class SqlExecContext {
    private final static Pattern tablePlaceHolderPattern = Pattern.compile("\\{(table\\d*)\\}");

    @Getter
    private final Connection connection;
    @Getter
    private final Map<String, String> tableMapping;

    private Statement statement;

    public SqlExecContext(Connection connection) {
        this(connection, new HashMap<>(1));
    }

    public String transSql(String sqlString) {
        Matcher matcher = tablePlaceHolderPattern.matcher(sqlString);
        while (matcher.find()) {
            String placeHolder = matcher.group(0);
            String placeHolderName = matcher.group(1);
            if (!tableMapping.containsKey(placeHolderName)) {
                tableMapping.put(placeHolderName, TableUtils.generateTableName());
            }
            sqlString = sqlString.replace(placeHolder, tableMapping.get(placeHolderName));
        }
        return sqlString;
    }

    /**
     * Drop the tables in context but not those pre-prepared.
     *
     * @param preparedTableMapping the pre-prepared tables
     * @throws SQLException sql exception
     */
    public void cleanUp(Map<String, String> preparedTableMapping) throws SQLException {
        Statement statement = getStatement();
        for (Map.Entry<String, String> entry : tableMapping.entrySet()) {
            if (!preparedTableMapping.containsKey(entry.getKey())) {
                statement.execute("drop table " + entry.getValue());
            }
        }
        statement.close();
    }

    public String getTableName(String placeHolderName) {
        return tableMapping.get(placeHolderName);
    }

    public void addTableMapping(String placeHolderName, String tableName) {
        tableMapping.put(placeHolderName, tableName);
    }

    public Statement getStatement() throws SQLException {
        if (statement!=null && !statement.isClosed()) {
            return statement;
        }
        statement = connection.createStatement();
        return statement;
    }

    public void closeStatement() throws SQLException {
        if (statement!=null && !statement.isClosed()) {
            statement.close();
        }
    }
}
