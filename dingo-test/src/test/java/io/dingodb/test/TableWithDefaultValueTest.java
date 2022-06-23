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

package io.dingodb.test;

import io.dingodb.common.table.TupleSchema;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Slf4j
public class TableWithDefaultValueTest {
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
    public void testCase00() throws Exception {
        String tableName = "test00";
        final String sqlCmd = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    amount double default 1.0,\n"
            + "    primary key(id)\n"
            + ")\n";
        sqlHelper.execSqlCmd(sqlCmd);
        String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);
        sqlHelper.clearTable(tableName);
    }

    @Test
    public void testCase01() throws Exception {
        String tableName = "test01";
        final String sqlCmd = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    amount double,\n"
            + "    primary key(id)\n"
            + ")";
        sqlHelper.execSqlCmd(sqlCmd);
        String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);

        sql = "select * from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            "100, lala, NULL");

        sql = "select id, name from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name"},
            TupleSchema.ofTypes("INTEGER", "STRING"),
            "100, lala");

        sqlHelper.clearTable(tableName);
    }

    @Test
    public void testCase02() throws Exception {
        String tableName = "test02";
        final String sqlCmd = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    amount double default 1.1,\n"
            + "    primary key(id)\n"
            + ")\n";
        sqlHelper.execSqlCmd(sqlCmd);
        String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);


        sql = "select * from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            "100, lala, 1.1");

        sql = "select id, name from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name"},
            TupleSchema.ofTypes("INTEGER", "STRING"),
            "100, lala");
        sqlHelper.clearTable(tableName);
    }

    @Test
    public void testCase03() throws Exception {
        String tableName = "test03";
        final String sqlCmd = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    score int default 100,\n"
            + "    primary key(id)\n"
            + ")\n";
        sqlHelper.execSqlCmd(sqlCmd);
        String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);
        sql = "update " + tableName + " set score = score - 10";
        sqlHelper.updateTest(sql, 1);

        sql = "select * from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name", "score"},
            TupleSchema.ofTypes("INTEGER", "STRING", "INTEGER"),
            "100, lala, 90");

        sql = "select score from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"score"},
            TupleSchema.ofTypes("INTEGER"),
            "90");
        sqlHelper.clearTable(tableName);
    }

    @Test
    public void testCase04() throws Exception {
        String tableName = "test04";
        final String sqlCmd = "create table " + tableName + " (\n"
            + "    id int not null,\n"
            + "    name varchar(32) not null,\n"
            + "    age int,\n"
            + "    amount double, \n"
            + "    address varchar(32),\n"
            + "    primary key(id)\n"
            + ")\n";
        sqlHelper.execSqlCmd(sqlCmd);
        String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);

        sql = "select * from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name", "age", "amount", "address"},
            TupleSchema.ofTypes("INTEGER", "STRING", "INTEGER", "DOUBLE", "STRING"),
            "100, lala, NULL, NULL, NULL");
        sqlHelper.clearTable(tableName);
    }

    @Test
    public void testCaseDefaultValueIsNull() throws Exception {
        String tableName = "testDefaultValueIsNull";
        final String sqlCmd = "create table " + tableName + " (\n"
            + "    id int not null,\n"
            + "    name varchar(32) not null,\n"
            + "    age int null default 20,\n"
            + " primary key(id))\n";
        sqlHelper.execSqlCmd(sqlCmd);

        String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.execSqlCmd(sql);

        sql = "select * from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name", "age"},
            TupleSchema.ofTypes("INTEGER", "STRING", "INTEGER", "DOUBLE", "STRING"),
            "100, lala, 20");
        sqlHelper.clearTable(tableName);
    }

    @Test
    public void testCase05() throws Exception {
        List<String> inputDateFuncList = Arrays.asList(
            "current_date",
            "current_date()",
            "curdate",
            "curdate()",
            "CURDATE",
            "CURRENT_DATE()");

        int index = 0;
        String tableNamePrefix = "table05";
        for (String funcName: inputDateFuncList) {
            String tableName = tableNamePrefix + index++;
            final String sqlCmd = "create table " + tableName + " (\n"
                + "    id int,\n"
                + "    name varchar(32) not null,\n"
                + "    birth date default " + funcName + " ,\n"
                + "    primary key(id)\n"
                + ")\n";
            sqlHelper.execSqlCmd(sqlCmd);
            String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
            sqlHelper.updateTest(sql, 1);
            sql = "select * from " + tableName;

            LocalDate nowDate = LocalDate.now();
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            String expectDateResult = nowDate.format(dateTimeFormatter);

            List<Object[]> expectedResult = new ArrayList<>();
            expectedResult.add(new Object[] {100, "lala", expectDateResult});
            sqlHelper.queryTestWithTime(sql,
                new String[]{"id", "name", "birth"},
                TupleSchema.ofTypes("INTEGER", "STRING", "DATE"),
                expectedResult);
            sqlHelper.clearTable(tableName);
        }
    }

    @Test
    public void testCase06() throws Exception {
        List<String> inputTimeFuncList = Arrays.asList(
            "current_time",
            "curtime",
            "current_time()",
            "curtime()",
            "CURTIME",
            "CURRENT_TIME()"
        );

        int index = 0;
        String tableNamePrefix = "table06";
        for (String funcName: inputTimeFuncList) {
            String tableName = tableNamePrefix + index++;
            final String sqlCmd = "create table " + tableName + " (\n"
                + "    id int,\n"
                + "    name varchar(32) not null,\n"
                + "    birth time default " + funcName + " ,\n"
                + "    primary key(id)\n"
                + ")\n";
            sqlHelper.execSqlCmd(sqlCmd);
            String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
            sqlHelper.updateTest(sql, 1);

            sql = "select * from " + tableName;
            LocalTime localTime = LocalTime.now();
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
            String formatTime = localTime.format(timeFormatter);
            System.out.println("=====Current time is: " + formatTime);
            List<Object[]> expectedResult = new ArrayList<>();
            expectedResult.add(new Object[] {100, "lala", formatTime});
            sqlHelper.queryTestWithTime(sql,
                new String[]{"id", "name", "birth"},
                TupleSchema.ofTypes("INTEGER", "STRING", "TIME"),
                expectedResult);
            sqlHelper.clearTable(tableName);
        }
    }

    @Test
    public void testCase07() throws Exception {
        List<String> inputTimeFuncList = Arrays.asList(
            "current_timestamp",
            "current_timestamp()"
        );

        int index = 0;
        String tableNamePrefix = "table07";
        for (String funcName: inputTimeFuncList) {
            String tableName = tableNamePrefix + index++;
            final String sqlCmd = "create table " + tableName + " (\n"
                + "    id int,\n"
                + "    name varchar(32) not null,\n"
                + "    birth timestamp default " + funcName + " ,\n"
                + "    primary key(id)\n"
                + ")\n";
            sqlHelper.execSqlCmd(sqlCmd);
            String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
            sqlHelper.updateTest(sql, 1);

            sql = "select * from " + tableName;
            LocalDateTime localDateTime = LocalDateTime.now();
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String formatTime = localDateTime.format(dateTimeFormatter);
            System.out.println("=====Current time is: " + formatTime);

            List<Object[]> expectedResult = new ArrayList<>();
            expectedResult.add(new Object[] {100, "lala", formatTime});
            sqlHelper.queryTestWithTime(sql,
                new String[]{"id", "name", "birth"},
                TupleSchema.ofTypes("INTEGER", "STRING", "TIMESTAMP"),
                expectedResult);
            sqlHelper.clearTable(tableName);
        }
    }

    @Test
    public void testCase08() throws Exception {
        String tableName = "testCase08";
        final String sqlCmd = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    address varchar(32) default lcase('ABC'), \n"
            + "    primary key(id)\n"
            + ")\n";
        sqlHelper.execSqlCmd(sqlCmd);
        String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);

        String expectRecord = "100, lala, abc";
        sql = "select * from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name", "address"},
            TupleSchema.ofTypes("INTEGER", "STRING", "STRING"),
            expectRecord);
        sqlHelper.clearTable(tableName);
    }

    @Test
    public void testCase09() throws Exception {
        String tableName = "testCase09";
        final String sqlCmd = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    address varchar(32) default concat('AAA','BBB'), \n"
            + "    primary key(id)\n"
            + ")\n";
        sqlHelper.execSqlCmd(sqlCmd);
        String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);

        String expectRecord = "100, lala, AAABBB";
        sql = "select * from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name", "address"},
            TupleSchema.ofTypes("INTEGER", "STRING", "STRING"),
            expectRecord);
        sqlHelper.clearTable(tableName);
    }

    @Test
    public void testCase10() throws Exception {
        String tableName = "testCase10";
        final String sqlCmd = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    age int default null, \n"
            + "    address varchar(32) default null, \n"
            + "    primary key(id)\n"
            + ")\n";
        sqlHelper.execSqlCmd(sqlCmd);
        String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);

        String expectRecord = "100, lala, null, null";
        sql = "select * from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name", "age", "address"},
            TupleSchema.ofTypes("INTEGER", "STRING", "INTEGER", "STRING"),
            expectRecord);
        sqlHelper.clearTable(tableName);
    }

    @Test
    public void testCase11() throws Exception {
        String tableName = "testCase11";
        String sqlCmd = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    age int default null, \n"
            + "    address varchar(32) default null, \n"
            + "    birthday timestamp not null default '2020-01-01 00:00:00', \n"
            + "    primary key(id))\n";
        sqlHelper.execSqlCmd(sqlCmd);

        String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);

        String expectRecord = "100, lala, null, null, 2020-01-01 00:00:00";
        sql = "select * from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name", "age", "address", "birthday"},
            TupleSchema.ofTypes("INTEGER", "STRING", "INTEGER", "STRING", "TIMESTAMP"),
            expectRecord);
        sqlHelper.clearTable(tableName);
    }

    @Test
    @Disabled
    public void testCase12() throws Exception {
        String tableName = "testCase12";
        String sqlCmd = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    age int default null, \n"
            + "    address varchar(32) default null, \n"
            + "    birth1 date not null default '2020-01-01', \n"
            + "    birth2 time not null default '10:30:30', \n"
            + "    birth3 timestamp not null default '2020-01-01 10:30:30', \n"
            + "    primary key(id))\n";
        sqlHelper.execSqlCmd(sqlCmd);

        String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);

        String expectRecord = "100, lala, null, null, 2020-01-01, 10:30:30, 2020-01-01 10:30:30";
        sql = "select * from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name", "age", "address", "birth1", "birth2", "birth3"},
            TupleSchema.ofTypes("INTEGER", "STRING", "INTEGER", "STRING", "DATE", "TIME", "TIMESTAMP"),
            expectRecord);
        sqlHelper.clearTable(tableName);
    }

    @Test
    @Disabled
    public void testCase13() throws Exception {
        String tableName = "testCase13";
        String sqlCmd = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32),\n"
            + "    age int, \n"
            + "    address varchar(32), \n"
            + "    birth1 date, \n"
            + "    birth2 time, \n"
            + "    birth3 timestamp, \n"
            + "    primary key(id))\n";

        sqlHelper.execSqlCmd(sqlCmd);
        String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);

        sql = "update " + tableName + " set address = 'beijing' where id = 100";
        sqlHelper.updateTest(sql, 1);
        String expectRecord01 = "100, lala, null, beijing, null, null, null";
        sql = "select * from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name", "age", "address", "birth1", "birth2", "birth3"},
            TupleSchema.ofTypes("INTEGER", "STRING", "INTEGER", "STRING", "DATE", "TIME", "TIMESTAMP"),
            expectRecord01);

        sql = "update " + tableName + " set birth1 = '2020-11-11' where id = 100";
        sqlHelper.updateTest(sql, 1);
        String expectRecord02 = "100, lala, null, beijing, 2020-11-11, null, null";
        sql = "select * from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name", "age", "address", "birth1", "birth2", "birth3"},
            TupleSchema.ofTypes("INTEGER", "STRING", "INTEGER", "STRING", "DATE", "TIME", "TIMESTAMP"),
            expectRecord02);

        sql = "update " + tableName + " set birth2 = '11:11:11' where id = 100";
        sqlHelper.updateTest(sql, 1);
        String expectRecord03 = "100, lala, null, beijing, 2020-11-11, 11:11:11, null";
        sql = "select * from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name", "age", "address", "birth1", "birth2", "birth3"},
            TupleSchema.ofTypes("INTEGER", "STRING", "INTEGER", "STRING", "DATE", "TIME", "TIMESTAMP"),
            expectRecord03);

        sql = "update " + tableName + " set birth3 = '2022-11-11 11:11:11' where id = 100";
        sqlHelper.updateTest(sql, 1);
        String expectRecord04 = "100, lala, null, beijing, 2020-11-11, 11:11:11, 2022-11-11 11:11:11";
        sql = "select * from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name", "age", "address", "birth1", "birth2", "birth3"},
            TupleSchema.ofTypes("INTEGER", "STRING", "INTEGER", "STRING", "DATE", "TIME", "TIMESTAMP"),
            expectRecord04);

        sqlHelper.clearTable(tableName);
    }

    @Test
    public void testCase14() throws Exception {
        String tableName = "testCase14";
        String sqlCmd = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32),\n"
            + "    birth1 date, \n"
            + "    birth2 time, \n"
            + "    birth3 timestamp, \n"
            + "    primary key(id))\n";

        sqlHelper.execSqlCmd(sqlCmd);
        String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);

        sql = "update " + tableName + " set birth1 = current_date() where id = 100";
        sqlHelper.updateTest(sql, 1);

        sql = "update " + tableName + " set birth2 = current_time() where id = 100";
        sqlHelper.updateTest(sql, 1);

        sql = "update " + tableName + " set birth3 = now() where id = 100";
        sqlHelper.updateTest(sql, 1);

        LocalDateTime localDateTime = LocalDateTime.now();
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String currentTimeStamp = localDateTime.format(dateTimeFormatter);
        System.out.println("=====Current time is: " + currentTimeStamp);
        LocalTime localTime = LocalTime.now();
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        String currentTime = localTime.format(timeFormatter);
        System.out.println("=====Current time is: " + currentTime);

        List<Object[]> expectedResult02 = new ArrayList<>();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String currentDate = simpleDateFormat.format(new Date());
        expectedResult02.add(new Object[] {100, "lala", currentDate, currentTime, currentTimeStamp});

        sql = "select * from " + tableName;
        sqlHelper.queryTestWithTime(
            sql,
            new String[]{"id", "name", "birth1", "birth2", "birth3"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DATE", "TIME", "TIMESTAMP"),
            expectedResult02);
        sqlHelper.clearTable(tableName);
    }

}
