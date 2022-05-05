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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class BasicTypeWithDateTest {
    private static final String TEST_ALL_DATA
        = "1, Alice, 2020-01-01\n"
        + "2, Betty, 2020-01-02\n";
    private static SqlHelper sqlHelper;
    private static Connection connection;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        connection = (sqlHelper = new SqlHelper()).getConnection();
        sqlHelper.execFile("/table-test-create-with-date.sql");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
        connection.close();
    }

    @BeforeEach
    public void setup() throws Exception {
        sqlHelper.execFile("/table-test-data-with-date.sql");
    }

    @AfterEach
    public void cleanUp() throws Exception {
        sqlHelper.clearTable("test");
    }

    @Test
    @Disabled("Failed to encoding Date.")
    public void testScan() throws SQLException, IOException {
        sqlHelper.queryTest("select * from test",
            new String[]{"id", "name", "birth"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DATE"),
            TEST_ALL_DATA
        );
    }

    @Test
    @Disabled("Failed to encoding Date")
    public void testScanTableWithConcatConst() throws SQLException, IOException {
        String prefix = "test-";
        String sql = "select '" + prefix + "' || birth from test where id = 1";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(">>" + rs.getString(1));
                    Assertions.assertEquals(rs.getString(1), prefix + "2020-01-01");
                }
            }
        }
    }

}
