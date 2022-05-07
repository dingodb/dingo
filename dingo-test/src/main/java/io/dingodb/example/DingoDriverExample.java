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
    private static final String defaultConnectIp = "172.20.3.13";
    private static Logger log = LoggerFactory.getLogger(DingoDriverExample.class);
    private static String connectUrl = "url=" + defaultConnectIp + ":8765";
    private static Connection connection;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java -jar io.dingodb.example.DingoDriverExample\r\n"
                + "\t\t 172.20.3.14 create exampletest\r\n"
                + "\t\t 172.20.3.14 insert exampletest 100000 10 startKey(default 1)\r\n"
                + "\t\t 172.20.3.14 query  exampletest");
            return;
        }

        String inputConnect = args[0];
        String command = args[1];
        String tableName = args[2];

        Statement statement = null;
        final Long startTime = System.currentTimeMillis();
        // default ip:172.20.3.13
        connectUrl = connectUrl.replace(defaultConnectIp, inputConnect);

        try {
            Class.forName("io.dingodb.driver.client.DingoDriverClient");
            connection = DriverManager.getConnection(DingoDriverClient.CONNECT_STRING_PREFIX + connectUrl);
            statement = connection.createStatement();
        } catch (ClassNotFoundException ex) {
            log.error("Init driver catch ClassNotFoundExeption:{}", ex.toString(), ex);
            return;
        } catch (SQLException ex) {
            log.error("Init driver catch SQLException:{}", ex.toString(), ex);
            return;
        } catch (Exception ex) {
            log.error("Init driver catch Exception:{}", ex.toString(), ex);
            return;
        }

        Long recordCnt = 100L;
        int batchCnt = 20;
        long startKey = 1L;

        if (args.length > 3) {
            recordCnt = Long.parseLong(args[3]);
        }
        if (args.length > 4) {
            batchCnt = Integer.parseInt(args[4]);
        }
        if (args.length > 5) {
            startKey = Long.parseLong(args[5]);
        }
        System.out.println("RecordCnt: " + recordCnt + ", batchCnt:" + batchCnt + ", startKey:" + startKey);

        try {
            switch (command.toUpperCase()) {
                case "CREATE": {
                    createTable(statement, tableName);
                    break;
                }
                case "INSERT": {
                    insertData(statement, tableName, recordCnt, batchCnt, startKey);
                    break;
                }
                case "QUERY": {
                    queryData(statement, tableName);
                    break;
                }
                default: {
                    log.info("Invalid input command:{}", command);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        final Long endTime = System.currentTimeMillis();
        System.out.println("Current Command:" + command
            + ", RecordCnt:" + recordCnt
            + ", AvgCost:" + (endTime - startTime) / recordCnt
            + "ms, totalCnt:" + recordCnt
            + ", batchCnt:" + batchCnt);
    }

    private static void createTable(final Statement statement, final String tableName) throws SQLException {
        String sql = "create table " + tableName + "("
            + "id int,"
            + "name varchar(32) not null,"
            + "age int,"
            + "amount double,"
            + "primary key(id)"
            + ")";
        statement.execute(sql);
    }

    private static void insertData(final Statement statement,
                                   final String tableName,
                                   final Long recordCnt,
                                   final int batchCnt,
                                   final Long startKey) throws SQLException {
        String initStr = "insert into " + tableName + " values ";
        String midStr = initStr;
        for (long i = startKey; i < recordCnt + 1; i++) {
            String row = "(" + i + ","
                + padLeftZeros(String.valueOf(i), 10) + ","
                + getRandomInt(10, 30) + ","
                + getRandomDouble(10, 30) + "),";
            midStr += row;
            if (i % batchCnt == 0 || i == recordCnt) {
                midStr = midStr.substring(0, midStr.length() - 1);
                int count = statement.executeUpdate(midStr);
                if (count != batchCnt) {
                    System.out.println("Insert Record Failed: Index:" + i + ",RealCnt:" + count);
                }
                midStr = initStr;
            }
        }
    }

    public static String padLeftZeros(final String inputString, int length) {
        if (inputString.length() >= length) {
            return inputString;
        }
        StringBuilder sb = new StringBuilder();
        while (sb.length() < length - inputString.length()) {
            sb.append('0');
        }
        sb.append(inputString);
        return sb.toString();
    }

    private static int getRandomInt(int min, int max) {
        return min + (int) (Math.random() * ((max - min) + 1));
    }

    private static double getRandomDouble(int min, int max) {
        return min + (Math.random() * ((max - min) + 1));
    }

    private static void queryData(final Statement statement, final String tableName) throws SQLException {
        String sql = "select * from " + tableName;

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
