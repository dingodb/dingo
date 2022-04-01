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

import io.dingodb.calcite.Connections;
import io.dingodb.exec.Services;
import io.dingodb.meta.test.MetaTestService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Slf4j
public class SimpleArithmeticTest {
    private static final String TEST_ALL_DATA
        = "1, Alice, 2020-01-01\n"
        + "2, Betty, 2020-01-02\n";
    private static Connection connection;
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        Services.metaServices.get(MetaTestService.SCHEMA_NAME).init(null);
        Services.initNetService();
        connection = Connections.getConnection(MetaTestService.SCHEMA_NAME);
        sqlHelper = new SqlHelper(connection);
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        connection.close();
        Services.metaServices.get(MetaTestService.SCHEMA_NAME).clear();
    }

    @Test
    public void testIntegerAdd() throws SQLException {
        String sql = "select 1 + 1";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getInt(1));
                }
            }
        }
    }

    @Test
    public void testBigDoubleAdd() throws SQLException {
        String sql = "select 1 + 100000000.2";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    assertEquals(rs.getDouble(1), Double.valueOf(1 + 100000000.2));
                }
            }
        }
    }

    @Test
    public void testSimpleDoubleAdd() throws SQLException {
        String sql = "select 1 + 100.1";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    assertEquals(rs.getDouble(1), Double.valueOf(101.1));
                }
            }
        }
    }

}
