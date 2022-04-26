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
import io.dingodb.common.util.StackTraces;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


@Slf4j
public class BasicTypeWithTimeTest {
    private static final String TEST_ALL_DATA
        = "1, Alice, 00:00:01\n"
        + "2, Betty, 00:01:02\n";
    private static SqlHelper sqlHelper;
    private static Connection connection;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        connection = (sqlHelper = new SqlHelper()).getConnection();
        sqlHelper.execFile("/table-test-create-with-time.sql");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
        connection.close();
    }

    @BeforeEach
    public void setup() throws Exception {
        sqlHelper.execFile("/table-test-data-with-time.sql");
    }

    @AfterEach
    public void cleanUp() throws Exception {
        sqlHelper.clearTable("test");
    }

    @Test
    public void testScan() throws SQLException, IOException {
        sqlHelper.queryTest("select * from test",
            new String[]{"id", "name", "birth"},
            TupleSchema.ofTypes("INTEGER", "STRING", "TIME"),
            TEST_ALL_DATA
        );
    }

    @Test
    public void testScanTableWithConcatConst() throws SQLException, IOException {
        String prefix = "test-";
        String sql = "select '" + prefix + "' || birth from test where id = 1";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(">>" + rs.getString(1));
                    Assertions.assertEquals(rs.getString(1), prefix + "00:00:01");
                }
            }
        }
    }

    @Test
    public void testScanTableWithConcatColumns() throws SQLException, IOException {
        String sql = "select id || name || birth from test where id = 1";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(">>" + rs.getString(1));
                    String realResult = rs.getString(1);
                    String expectedResult = "1Alice00:00:01";
                    Assertions.assertEquals(realResult, expectedResult);
                }
            }
        }
    }
}
