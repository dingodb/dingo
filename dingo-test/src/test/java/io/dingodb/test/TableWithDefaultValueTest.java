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
import org.junit.jupiter.api.Test;

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
    public void testCase05() throws Exception {
        // FIXME: null value when default value is empty
        /*
        String tableName = "test04";
        final String sqlCmd = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    birth date default current_date,\n"
            + "    primary key(id)\n"
            + ")\n";
        sqlHelper.execSqlCmd(sqlCmd);
        String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        sqlHelper.updateTest(sql, 1);
        sql = "select * from " + tableName;
        sqlHelper.queryTest(sql,
            new String[]{"id", "name", "birth"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DATE"),
            "100, lala, 2022-04-20");
        sqlHelper.clearTable(tableName);
       */
    }
}
