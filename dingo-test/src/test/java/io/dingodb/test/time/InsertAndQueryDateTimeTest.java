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

package io.dingodb.test.time;

import io.dingodb.test.SqlHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;

@Slf4j
public class InsertAndQueryDateTimeTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @Test
    public void testInsertDateAndFormatOutput() throws SQLException {
        String tableName = SqlHelper.randomTableName();
        sqlHelper.execSqlCmd(
            "create table " + tableName + "("
                + "id int,"
                + "name varchar(32) not null,"
                + "age int,"
                + "amount double,"
                + "address varchar(255),"
                + "birthday date,"
                + "createTime time,"
                + "update_Time timestamp,"
                + "primary key(id)"
                + ")"
        );
        sqlHelper.updateTest(
            "insert into " + tableName + " values "
                + "(1,'zhangsan',18,23.50,'beijing','20220401', '08:10:10', '2022-4-8 18:05:07'),"
                + "(2,'lisi',25,895,' beijing haidian ', '1988-2-05', '06:15:8', '2000-02-29 00:00:00'),"
                + "(3,'l3',55,123.123,'wuhan NO.1 Street', '2022-03-4', '07:3:15', '1999-2-28 23:59:59'),"
                + "(4,'HAHA',57,9.0762556,'CHANGping', '2020-11-11', '5:59:59', '2021-05-04 12:00:00'),"
                + "(5,'awJDs',1,1453.9999,'pingYang1', '2010-10-1', '19:0:0', '2010-10-1 02:02:02'),"
                + "(6,'123',544,0,'543', '1987-7-16', '1:2:3', '1952-12-31 12:12:12'),"
                + "(7,'yamaha',76,2.30,'beijing changyang', '1949-01-01', '0:30:8', '2022-12-01 1:2:3')",
            7
        );
        sqlHelper.queryTest(
            "select name, timestamp_format(update_time, '%Y/%m/%d %H.%i.%s') ts_out from "
                + tableName + " where id=1",
            new String[]{"name", "ts_out"},
            Collections.singletonList(new Object[]{"zhangsan", "2022/04/08 18.05.07"})
        );
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testInsertTime() throws SQLException {
        String tableName = SqlHelper.randomTableName();
        sqlHelper.execSqlCmd(
            "create table " + tableName + "("
                + "id int,"
                + "name varchar(20),"
                + "age int,"
                + "amount double,"
                + "create_time time," +
                "primary key (id)"
                + ")"
        );
        sqlHelper.updateTest(
            "insert into " + tableName + " values(1, 'zhang san', 18, 1342.09, '112341')",
            1
        );
        sqlHelper.queryTest(
            "SELECT * from " + tableName,
            new String[]{"id", "name", "age", "amount", "create_time"},
            Collections.singletonList(new Object[]{1, "zhang san", 18, 1342.09, "11:23:41"})
        );
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testInsertTimeAndFormatOutput() throws SQLException {
        String tableName = SqlHelper.randomTableName();
        sqlHelper.execSqlCmd(
            "create table " + tableName + "("
                + "id int,"
                + "name varchar(20),"
                + "age int,"
                + "amount double,"
                + "address varchar(255),"
                + "update_time time,"
                + "primary key (id)"
                + ")"
        );
        sqlHelper.updateTest(
            "insert into " + tableName + " values(1,'aa',18,2.5,'beijing','17:38:28')",
            1
        );
        sqlHelper.queryTest(
            "select name, time_format(update_time, '%H:%i:%s') time_out from " + tableName,
            new String[]{"name", "time_out"},
            Collections.singletonList(new Object[]{"aa", "17:38:28"})
        );
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testInsertDate() throws SQLException {
        String tableName = SqlHelper.randomTableName();
        sqlHelper.execSqlCmd(
            "create table " + tableName + "("
                + "id int,"
                + "name varchar(20),"
                + "age int,"
                + "amount double,"
                + "create_date date,"
                + "primary key (id)"
                + ")"
        );
        sqlHelper.updateTest(
            "insert into " + tableName + " values(1, 'zhang san', 18, 1342.09, '2020.01.01')",
            1
        );
        sqlHelper.queryTest(
            "SELECT * from " + tableName,
            new String[]{"id", "name", "age", "amount", "create_date"},
            Collections.singletonList(new Object[]{1, "zhang san", 18, 1342.09, "2020-01-01"})
        );
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testInsertTimestamp() throws SQLException {
        String tableName = SqlHelper.randomTableName();
        sqlHelper.execSqlCmd(
            "create table " + tableName + "("
                + "id int,"
                + "create_time time,"
                + "update_time timestamp,"
                + "primary key(id)"
                + ")"
        );
        sqlHelper.updateTest(
            "insert into " + tableName + " values"
                + "(1,'14:00:00','2022-06-22 10:10:59'),"
                + "(2,'01:13:06','1949-10-01 01:00:00'),"
                + "(3,'23:59:59','1987-07-16 00:00:00')",
            3
        );
        sqlHelper.queryTest("SELECT * from " + tableName + " where id = 2",
            new String[]{"id", "create_time", "update_time"},
            Collections.singletonList(new Object[]{2, "01:13:06", Timestamp.valueOf("1949-10-01 01:00:00")})
        );
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testSelectTimeStamp() throws SQLException {
        String tableName = SqlHelper.randomTableName();
        sqlHelper.execSqlCmd(
            "create table " + tableName + "("
                + "id int,"
                + "name varchar(32) not null,"
                + "age int,"
                + "amount double,"
                + "address varchar(255),"
                + "birthday date,"
                + "create_time time,"
                + "update_time timestamp,"
                + "primary key(id)"
                + ")"
        );
        sqlHelper.updateTest(
            "insert into " + tableName + " values "
                + "(1, 'zhangsan', 18,23.50, 'beijing','1998-4-6', '08:10:10', '2022-4-8 18:05:07')",
            1
        );
        sqlHelper.queryTest(
            "select unix_timestamp(update_time) as ts from " + tableName + " limit 1",
            new String[]{"ts"},
            Collections.singletonList(new Object[]{Timestamp.valueOf("2022-04-08 18:05:07").getTime() / 1000L})
        );
    }
}
