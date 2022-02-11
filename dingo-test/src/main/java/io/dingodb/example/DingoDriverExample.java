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

package io.dingodb.example;

import io.dingodb.driver.client.DingoDriverClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DingoDriverExample {
    private static Logger log = LoggerFactory.getLogger(DingoDriverExample.class);

    private static Connection connection;

    private static String connectUrl = "url=http://172.20.3.200:8765";

    public static void main(String[] args) throws Exception {
        Class.forName("io.dingodb.driver.client.DingoDriverClient");
        connection = DriverManager.getConnection(
            DingoDriverClient.CONNECT_STRING_PREFIX + connectUrl
        );

        Statement statement = connection.createStatement();

        createTable(statement);
        insertData(statement);
        queryData(statement);

        if (statement != null) {
            statement.close();
        }

        if (connection != null) {
            connection.close();
        }
    }

    private static void createTable(Statement statement) throws Exception {
        String sql = "create table exampleTest ("
            + "id int,"
            + "name varchar(32) not null,"
            + "age int,"
            + "amount double,"
            + "primary key(id)"
            + ")";
        statement.execute(sql);
    }

    private static void insertData(Statement statement) throws Exception {
        String sql = "insert into exampleTest values (1, 'example001', 19, 11.0)";
        int count = statement.executeUpdate(sql);
        log.info("Insert data count = [{}]", count);
    }

    private static void queryData(Statement statement) throws Exception {
        String sql = "select * from exampleTest";

        try (ResultSet resultSet = statement.executeQuery(sql)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            List<Map<String, String>> result = new ArrayList<>();
            int columnCount = metaData.getColumnCount();
            int line = 1;
            while (resultSet.next()) {
                Map<String, String> lineMap = new HashMap<>();
                for (int i = 1; i < columnCount + 1; i++) {
                    try {
                        System.out.printf("line = " + line + "   column = " + metaData.getColumnName(i)
                            + "=" + resultSet.getString(i));
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
        }
    }
}
