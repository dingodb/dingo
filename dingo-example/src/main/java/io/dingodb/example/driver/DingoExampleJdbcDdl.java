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

package io.dingodb.example.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DingoExampleJdbcDdl {
    private static Logger log = LoggerFactory.getLogger(DingoExampleUsingJdbc.class);

    public static void main(String[] args) throws SQLException {
        if (args.length < 2) {
            System.out.println("Usage: java -jar io.dingodb.example.driver.DingoExampleJdbcDdl \r\n"
                + "\t\t 172.20.3.14 dingo 123123 show user host");
            return;
        }

        Connection connection = DingoExampleQueryTables.getConnection(args);
        String command = args[3];
        String user = args[4];
        String host = args[5];
        Statement statement = null;
        try {
            statement = connection.createStatement();
            switch (command.toUpperCase()) {
                case "SHOW":
                    showGrant(statement, user, host);
                    break;
                default:
                    log.info("Invalid input command:{}", command);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    public static void showGrant(final Statement statement, String user, String host) throws SQLException {
        String sql = "show grants for " + user + "@" + "'" + host + "'";
        ResultSet resultSet = null;
        try {
            resultSet = statement.executeQuery(sql);
            ResultSetMetaData metaData = resultSet.getMetaData();
            List<Map<String, String>> result = new ArrayList<>();
            int columnCount = metaData.getColumnCount();
            int line = 1;
            while (resultSet.next()) {
                Map<String, String> lineMap = new HashMap<>();
                for (int i = 1; i < columnCount + 1; i++) {
                    try {
                        log.info("Exec sql [{}]: line=[{}], column [{}]=[{}].", "sss", line,
                            metaData.getColumnName(i), resultSet.getString(i));
                        lineMap.put(metaData.getColumnName(i), resultSet.getString(i));
                    } catch (SQLException e) {
                        log.error("Get sql result column error, column index: [{}].", i + 1, e);
                    }
                }
                line++;
                result.add(lineMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            resultSet.close();
        }
    }
}
