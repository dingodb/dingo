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

import io.dingodb.driver.client.DingoDriverClient;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DingoExampleQueryTables {

    private static final String defaultConnectIp = "172.20.3.13";
    private static String connectUrl = "url=" + defaultConnectIp + ":8765";

    public static Connection getConnection(String[] args) {
        try {
            String inputConnect = args[0];
            String user = args[1];
            String pwd = args[2];
            connectUrl = connectUrl.replace(defaultConnectIp, inputConnect);
            Class.forName("io.dingodb.driver.client.DingoDriverClient");
            Connection connection = DriverManager.getConnection(DingoDriverClient.CONNECT_STRING_PREFIX + connectUrl,
                user, pwd);
            return connection;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }

    }

    public static void main(String[] args) throws SQLException {
        if (args.length < 2) {
            System.out.println("Usage: java -jar io.dingodb.example.driver.DingoExampleQueryTables \r\n"
                + "\t\t 172.20.3.14 dingo 123123");
            return;
        }
        Connection connection = getConnection(args);
        DatabaseMetaData dmd = connection.getMetaData();
        ResultSet resultSetSchema = dmd.getSchemas();
        List<String> schemaList = new ArrayList<>();
        while (resultSetSchema.next()) {
            schemaList.add(resultSetSchema.getString(1));
        }

        List<String> tableList = new ArrayList<String>();
        ResultSet rst = dmd.getTables(null, "DINGO", "%", null);
        while (rst.next()) {
            tableList.add(rst.getString("TABLE_NAME").toUpperCase());
        }
        rst.close();
        resultSetSchema.close();
        for (String table : tableList) {
            System.out.println("----> table:" + table);
        }
    }
}
