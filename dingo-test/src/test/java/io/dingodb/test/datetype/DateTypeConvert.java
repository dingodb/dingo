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
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DateTypeConvert {
    private static Connection connection;
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        connection = (sqlHelper = new SqlHelper()).getConnection();
    }

    @Test
    @Disabled
    public void testSelectTimeStamp() throws SQLException {
        String dropSql = "DROP table  IF EXISTS  tesxx";

        String createTableSql = "create table testxx ("
            + "id int,"
            + "name varchar(32) not null,"
            + "age int,"
            + "amount double,"
            + "address varchar(255),"
            + "birthday date,"
            + "createTime time,"
            + "update_Time timestamp,"
            + "primary key(id)"
            + ")";
        String insertTableSql = "insert into testxx"
            + " values "
            + "(1,'zhangsan',18,23.50,'beijing','1998-4-6', '08:10:10', '2022-4-8 18:05:07')";
        //        +"(2,'lisi',25,895,' beijing haidian ', '1988-2-05', '06:15:8', '2000-02-29 00:00:00'),\n"
        //        +"(3,'l3',55,123.123,'wuhan NO.1 Street', '2022-03-4', '07:3:15', '1999-2-28 23:59:59'),\n"
        //        +"(4,'HAHA',57,9.0762556,'CHANGping', '2020-11-11', '5:59:59', '2021-05-04 12:00:00'),\n"
        //        +"(5,'awJDs',1,1453.9999,'pingYang1', '2010-10-1', '19:0:0', '2010-10-1 02:02:02'),\n"
        //        +"(6,'123',544,0,'543', '1987-7-16', '1:2:3', '1952-12-31 12:12:12'),\n"
        //        +"(7,'yamaha',76,2.30,'beijing changyang', '1949-01-01', '0:30:8', '2022-12-01 1:2:3')";
        try (Statement statement = connection.createStatement()) {
            statement.execute(dropSql);
            statement.execute(createTableSql);
            statement.executeUpdate(insertTableSql);
        }
        System.out.println("=========================================");

        String selectTableSql = "select unix_timestamp(update_time) from testxx limit 1";
        //        String selectTableSql = "select unix_timestamp(cast update_time) from test limit 1";

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(selectTableSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    String result = rs.getString(1);
                    System.out.println(result);
                    assertThat(rs.getString(1)).isEqualTo("1649412307");
                }
            }
        }
    }
}
