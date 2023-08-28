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

import io.dingodb.test.dsl.run.check.CheckContext;
import io.dingodb.test.utils.ResourceFileUtils;
import io.dingodb.test.utils.ResultSetUtils;
import io.dingodb.test.utils.TableUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

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
                try {
                    statement.execute("drop table if exists " + entry.getValue());
                } catch (Exception ignored) {
                    // This is not part of testing, so exception is allowed.
                }
            }
        }
        statement.close();
    }

    /**
     * Drop the tables in context.
     *
     * @throws SQLException sql exception
     */
    public void cleanUp() throws SQLException {
        Statement statement = getStatement();
        for (Map.Entry<String, String> entry : tableMapping.entrySet()) {
            try {
                statement.execute("drop table if exists " + entry.getValue());
            } catch (Exception ignored) {
                // This is not part of testing, so exception is allowed.
            }
        }
        statement.close();
    }

    public CheckContext execSql(String sqlString) throws SQLException {
        Boolean b = null;
        String sql = "";
        Exception exception = null;
        Statement statement = getStatement();
        try {
            for (String s : sqlString.split(";")) {
                if (!s.trim().isEmpty()) {
                    b = statement.execute(transSql(s));
                    sql = s;
                }
            }
        } catch (Exception e) {
            exception = e;
        }
        if (exception == null) {
            assertThat(b).isNotNull();
        }
        return new CheckContext(statement, b, sql, exception);
    }

    public CheckContext execSql(InputStream stream) throws SQLException {
        return execSql(ResourceFileUtils.readString(stream));
    }

    public Object[] querySingleRow(String sqlString) throws SQLException {
        CheckContext context = execSql(sqlString);
        try (ResultSet resultSet = context.getStatement().getResultSet()) {
            int count = 0;
            Object[] row = null;
            while (resultSet.next()) {
                ++count;
                row = ResultSetUtils.getRow(resultSet);
            }
            assertThat(count).isEqualTo(1);
            return row;
        }
    }

    public Object querySingleValue(String sqlString) throws SQLException {
        Object[] tuple = querySingleRow(sqlString);
        assertThat(tuple).hasSize(1);
        return tuple[0];
    }

    public String getTableName(String placeHolderName) {
        return tableMapping.get(placeHolderName);
    }

    public void addTableMapping(String placeHolderName, String tableName) {
        tableMapping.put(placeHolderName, tableName);
    }

    public Statement getStatement() throws SQLException {
        if (statement != null && !statement.isClosed()) {
            return statement;
        }
        statement = connection.createStatement();
        return statement;
    }
}
