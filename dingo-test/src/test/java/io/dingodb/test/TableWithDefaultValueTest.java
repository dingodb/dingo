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

import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.UUID;

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

    private static String randomTableName() {
        return UUID.randomUUID().toString().replace('-', '_');
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
        sqlHelper.dropTable(tableName);
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
            DingoTypeFactory.tuple("INTEGER", "STRING", "DOUBLE"),
            "100, lala, NULL");

        sql = "select id, name from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            "100, lala");

        sqlHelper.dropTable(tableName);
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
            DingoTypeFactory.tuple("INTEGER", "STRING", "DOUBLE"),
            "100, lala, 1.1");

        sql = "select id, name from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            "100, lala");
        sqlHelper.dropTable(tableName);
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
            DingoTypeFactory.tuple("INTEGER", "STRING", "INTEGER"),
            "100, lala, 90");

        sql = "select score from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"score"},
            DingoTypeFactory.tuple("INTEGER"),
            "90");
        sqlHelper.dropTable(tableName);
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
            DingoTypeFactory.tuple("INTEGER", "STRING", "INTEGER", "DOUBLE", "STRING"),
            "100, lala, NULL, NULL, NULL");
        sqlHelper.dropTable(tableName);
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
            DingoTypeFactory.tuple("INTEGER", "STRING", "INTEGER"),
            "100, lala, 20");
        sqlHelper.dropTable(tableName);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "current_date",
        "current_date()",
        "curdate()",
        "CURRENT_DATE()",
    })
    public void testDefaultCurrentDate(String fun) throws SQLException {
        String tableName = "table_curdate_" + randomTableName();
        String sql = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    birth date default " + fun + " ,\n"
            + "    primary key(id)\n"
            + ")\n";
        sqlHelper.execSqlCmd(sql);
        sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);
        sql = "select * from " + tableName;
        sqlHelper.queryTestInOrderWithApproxTime(
            sql,
            new String[]{"id", "name", "birth"},
            Collections.singletonList(new Object[]{100, "lala", Date.valueOf(LocalDate.now())})
        );
        sqlHelper.dropTable(tableName);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "current_time",
        "current_time()",
        "curtime()",
        "CURRENT_TIME()"
    })
    public void testDefaultCurrentTime(String fun) throws Exception {
        String tableName = "table_curtime_" + randomTableName();
        String sql = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    birth time default " + fun + " ,\n"
            + "    primary key(id)\n"
            + ")\n";
        sqlHelper.execSqlCmd(sql);
        sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);
        sql = "select * from " + tableName;
        sqlHelper.queryTestInOrderWithApproxTime(
            sql,
            new String[]{"id", "name", "birth"},
            Collections.singletonList(new Object[]{100, "lala", DateTimeUtils.currentTime()})
        );
        sqlHelper.dropTable(tableName);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "now()",
        "current_timestamp",
        "current_timestamp()",
    })
    public void testDefaultCurrentTimestamp(String fun) throws Exception {
        String tableName = "table_now_" + randomTableName();
        String sql = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    birth timestamp default " + fun + " ,\n"
            + "    primary key(id)\n"
            + ")\n";
        sqlHelper.execSqlCmd(sql);
        sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);
        sql = "select * from " + tableName;
        sqlHelper.queryTestInOrderWithApproxTime(
            sql,
            new String[]{"id", "name", "birth"},
            Collections.singletonList(new Object[]{100, "lala", Timestamp.valueOf(LocalDateTime.now())})
        );
        sqlHelper.dropTable(tableName);
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
            DingoTypeFactory.tuple("INTEGER", "STRING", "STRING"),
            expectRecord);
        sqlHelper.dropTable(tableName);
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
            DingoTypeFactory.tuple("INTEGER", "STRING", "STRING"),
            expectRecord);
        sqlHelper.dropTable(tableName);
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
            DingoTypeFactory.tuple("INTEGER", "STRING", "INTEGER", "STRING"),
            expectRecord);
        sqlHelper.dropTable(tableName);
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
            DingoTypeFactory.tuple("INTEGER", "STRING", "INTEGER", "STRING", "TIMESTAMP"),
            expectRecord);
        sqlHelper.dropTable(tableName);
    }

    @Test
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
        sqlHelper.queryTestInOrderWithApproxTime(sql,
            new String[]{"id", "name", "age", "address", "birth1", "birth2", "birth3"},
            DingoTypeFactory.tuple("INTEGER", "STRING", "INTEGER", "STRING", "DATE", "TIME", "TIMESTAMP"),
            expectRecord);
        sqlHelper.dropTable(tableName);
    }

    @Test
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
        sqlHelper.queryTestInOrderWithApproxTime(sql,
            new String[]{"id", "name", "age", "address", "birth1", "birth2", "birth3"},
            DingoTypeFactory.tuple("INTEGER", "STRING", "INTEGER", "STRING", "DATE", "TIME", "TIMESTAMP"),
            expectRecord01);

        sql = "update " + tableName + " set birth1 = '2020-11-11' where id = 100";
        sqlHelper.updateTest(sql, 1);
        String expectRecord02 = "100, lala, null, beijing, 2020-11-11, null, null";
        sql = "select * from " + tableName;
        sqlHelper.queryTestInOrderWithApproxTime(sql,
            new String[]{"id", "name", "age", "address", "birth1", "birth2", "birth3"},
            DingoTypeFactory.tuple("INTEGER", "STRING", "INTEGER", "STRING", "DATE", "TIME", "TIMESTAMP"),
            expectRecord02);

        sql = "update " + tableName + " set birth2 = '11:11:11' where id = 100";
        sqlHelper.updateTest(sql, 1);
        String expectRecord03 = "100, lala, null, beijing, 2020-11-11, 11:11:11, null";
        sql = "select * from " + tableName;
        sqlHelper.queryTestInOrderWithApproxTime(sql,
            new String[]{"id", "name", "age", "address", "birth1", "birth2", "birth3"},
            DingoTypeFactory.tuple("INTEGER", "STRING", "INTEGER", "STRING", "DATE", "TIME", "TIMESTAMP"),
            expectRecord03);

        sql = "update " + tableName + " set birth3 = '2022-11-11 11:11:11' where id = 100";
        sqlHelper.updateTest(sql, 1);
        String expectRecord04 = "100, lala, null, beijing, 2020-11-11, 11:11:11, 2022-11-11 11:11:11";
        sql = "select * from " + tableName;
        sqlHelper.queryTestInOrderWithApproxTime(sql,
            new String[]{"id", "name", "age", "address", "birth1", "birth2", "birth3"},
            DingoTypeFactory.tuple("INTEGER", "STRING", "INTEGER", "STRING", "DATE", "TIME", "TIMESTAMP"),
            expectRecord04);

        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testCase14() throws Exception {
        String tableName = "testCase14";
        String sql = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32),\n"
            + "    birth1 date, \n"
            + "    birth2 time, \n"
            + "    birth3 timestamp, \n"
            + "    primary key(id)\n"
            + ")\n";

        sqlHelper.execSqlCmd(sql);
        sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);

        sql = "update " + tableName + " set birth1 = current_date() where id = 100";
        sqlHelper.updateTest(sql, 1);

        sql = "update " + tableName + " set birth2 = current_time() where id = 100";
        sqlHelper.updateTest(sql, 1);

        sql = "update " + tableName + " set birth3 = now() where id = 100";
        sqlHelper.updateTest(sql, 1);

        sql = "select * from " + tableName;
        sqlHelper.queryTestInOrderWithApproxTime(
            sql,
            new String[]{"id", "name", "birth1", "birth2", "birth3"},
            Collections.singletonList(
                new Object[]{
                    100,
                    "lala",
                    Date.valueOf(LocalDate.now()),
                    DateTimeUtils.currentTime(),
                    Timestamp.valueOf(LocalDateTime.now())
                }
            )
        );
        sqlHelper.dropTable(tableName);
    }
}
