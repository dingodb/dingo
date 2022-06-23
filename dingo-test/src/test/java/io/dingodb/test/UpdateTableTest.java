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
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

@Slf4j
public class UpdateTableTest {
    private static final String TEST_ALL_DATA
        = "1, Alice, 3.5\n"
        + "2, Betty, 4.0\n"
        + "3, Cindy, 4.5\n"
        + "4, Doris, 5.0\n"
        + "5, Emily, 5.5\n"
        + "6, Alice, 6.0\n"
        + "7, Betty, 6.5\n"
        + "8, Alice, 7.0\n"
        + "9, Cindy, 7.5\n";
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-create.sql");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    private static void checkDatumInTestTable(String data) throws SQLException {
        sqlHelper.queryTest("select * from test",
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            data
        );
    }

    @BeforeEach
    public void setup() throws Exception {
        sqlHelper.execFile("/table-test-data.sql");
    }

    @AfterEach
    public void cleanUp() throws Exception {
        sqlHelper.clearTable("test");
    }

    @Test
    public void testUpdate() throws SQLException {
        String sql = "update test set amount = 100 where id = 1";
        sqlHelper.updateTest(sql, 1);
        checkDatumInTestTable(
            "1, Alice, 100.0\n"
                + "2, Betty, 4.0\n"
                + "3, Cindy, 4.5\n"
                + "4, Doris, 5.0\n"
                + "5, Emily, 5.5\n"
                + "6, Alice, 6.0\n"
                + "7, Betty, 6.5\n"
                + "8, Alice, 7.0\n"
                + "9, Cindy, 7.5\n"
        );
    }

    @Test
    public void testUpdate1() throws SQLException {
        String sql = "update test set amount = amount + 100";
        sqlHelper.updateTest(sql, 9);
        checkDatumInTestTable(
            "1, Alice, 103.5\n"
                + "2, Betty, 104.0\n"
                + "3, Cindy, 104.5\n"
                + "4, Doris, 105.0\n"
                + "5, Emily, 105.5\n"
                + "6, Alice, 106.0\n"
                + "7, Betty, 106.5\n"
                + "8, Alice, 107.0\n"
                + "9, Cindy, 107.5\n"
        );
    }

    @Test
    public void testDelete() throws SQLException {
        String sql = "delete from test where id = 3 or id = 4";
        sqlHelper.updateTest(sql, 2);
        checkDatumInTestTable(
            "1, Alice, 3.5\n"
                + "2, Betty, 4.0\n"
                + "5, Emily, 5.5\n"
                + "6, Alice, 6.0\n"
                + "7, Betty, 6.5\n"
                + "8, Alice, 7.0\n"
                + "9, Cindy, 7.5\n"
        );
    }

    @Test
    public void testDelete1() throws SQLException {
        String sql = "delete from test where name = 'Alice'";
        sqlHelper.updateTest(sql, 3);
        checkDatumInTestTable(
            "2, Betty, 4.0\n"
                + "3, Cindy, 4.5\n"
                + "4, Doris, 5.0\n"
                + "5, Emily, 5.5\n"
                + "7, Betty, 6.5\n"
                + "9, Cindy, 7.5\n"
        );
    }

    @Test
    public void testInsert() throws SQLException {
        String sql = "insert into test values(10, 'Alice', 8.0), (11, 'Cindy', 8.5)";
        sqlHelper.updateTest(sql, 2);
        checkDatumInTestTable(
            TEST_ALL_DATA
                + "10, Alice, 8.0\n"
                + "11, Cindy, 8.5\n"
        );
    }

    @Test
    public void testUpdateWithException() throws SQLException {
        String sql = "update test set amount = 'xxxx' where id = 1";
        try {
            sqlHelper.updateTest(sql, 1);
        } catch (Exception ex) {
            Assertions.assertTrue(ex.toString().contains("NumberFormatException"));
        }
    }

}
