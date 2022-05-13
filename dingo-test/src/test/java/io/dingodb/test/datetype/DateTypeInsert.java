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

package io.dingodb.test.datetype;

import io.dingodb.test.SqlHelper;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DateTypeInsert {
    private static Connection connection;
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        connection = (sqlHelper = new SqlHelper()).getConnection();
    }

    @Test
    @Disabled
    public void testSelectTimeStamp() throws SQLException {
        String dropSql = "DROP table  IF EXISTS  timetest01";

        String createTableSql = "create table timetest01(id int, name varchar(20), age int, amount double,"
            + " create_time time, primary key (id))";
        String insertTableSql = "insert into timetest01 values(1, 'zhang san', 18, 1342.09, '123015'),"
            + "(2, 'li si', 20, 13.42, '005959')";
        try (Statement statement = connection.createStatement()) {
            statement.execute(dropSql);
            statement.execute(createTableSql);
            statement.executeUpdate(insertTableSql);
        }
        System.out.println("=========================================");

        //String selectTableSql = "select unix_timestamp(update_time) from testxx limit 1";
        String selectTableSql = "select * from timetest01";

        //        String selectTableSql = "select * from testxx limit 1";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(selectTableSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                }
            }
        }
    }
}
