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

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TableWithDefaultValueTest {

    private static java.sql.Connection connection;
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        io.dingodb.exec.Services.metaServices.get(io.dingodb.meta.test.MetaTestService.SCHEMA_NAME).init(null);
        io.dingodb.exec.Services.initNetService();
        connection = io.dingodb.calcite.Connections.getConnection(io.dingodb.meta.test.MetaTestService.SCHEMA_NAME);
        sqlHelper = new SqlHelper(connection);
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        connection.close();
        io.dingodb.exec.Services.metaServices.get(io.dingodb.meta.test.MetaTestService.SCHEMA_NAME).clear();
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
        try (java.sql.Statement statement = connection.createStatement()) {
            int cnt = statement.executeUpdate(sql);
            assertThat(cnt).isEqualTo(1);
        }
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
        try (java.sql.Statement statement = connection.createStatement()) {
            int cnt = statement.executeUpdate(sql);
            assertThat(cnt).isEqualTo(1);
        }
        sqlHelper.clearTable(tableName);
    }

    // @Test
    public void testCase02() throws Exception {
        String tableName = "test02";
        final String sqlCmd = "create table " + tableName + " (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    amount double not null default 1.1,\n"
            + "    primary key(id)\n"
            + ")\n";
        sqlHelper.execSqlCmd(sqlCmd);
        String sql = "insert into " + tableName + " (id, name) values (100, 'lala')";
        try (java.sql.Statement statement = connection.createStatement()) {
            int cnt = statement.executeUpdate(sql);
            assertThat(cnt).isEqualTo(1);
        }
        sqlHelper.clearTable(tableName);
    }

}
