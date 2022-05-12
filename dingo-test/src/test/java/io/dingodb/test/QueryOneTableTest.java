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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class QueryOneTableTest {
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
        sqlHelper.execFile("/table-test-data.sql");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @BeforeEach
    public void setup() throws Exception {
    }

    @AfterEach
    public void cleanUp() throws Exception {
    }

    @Test
    public void testScan() throws SQLException {
        sqlHelper.queryTest(
            "select * from test",
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            TEST_ALL_DATA
        );
    }

    @Test
    public void testScan1() throws SQLException {
        sqlHelper.queryTest(
            "select * from test.test",
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            TEST_ALL_DATA
        );
    }

    @Test
    public void testGetByKey() throws SQLException {
        String sql = "select * from test where id = 1";
        sqlHelper.queryTest(
            sql,
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            "1, Alice, 3.5\n"
        );
    }

    @Test
    public void testGetByKey1() throws SQLException {
        String sql = "select * from test where id = 1 or id = 2";
        sqlHelper.queryTest(
            sql,
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            "1, Alice, 3.5\n"
                + "2, Betty, 4.0\n"
        );
    }

    @Test
    public void testGetByKey2() throws SQLException {
        String sql = "select * from test where id in (1, 2, 3)";
        sqlHelper.queryTest(
            sql,
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            "1, Alice, 3.5\n"
                + "2, Betty, 4.0\n"
                + "3, Cindy, 4.5\n"
        );
    }

    @Test
    public void testScanRecordsWithNotInCause() throws SQLException {
        String sql = "select * from test where id not in (3, 4, 5, 6, 7, 8, 9)";
        sqlHelper.queryTest(
            sql,
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            "1, Alice, 3.5\n"
                + "2, Betty, 4.0\n"
        );
    }

    @Test
    public void testScanWithMultiCondition() throws SQLException {
        String sql = "select * from test where id > 1 and name = 'Alice' and amount > 6";
        sqlHelper.queryTest(
            sql,
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            "8, Alice, 7.0\n"
        );
    }


    @Test
    public void testFilterScan() throws SQLException {
        String sql = "select * from test where amount > 4.0";
        sqlHelper.queryTest(
            sql,
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            "3, Cindy, 4.5\n"
                + "4, Doris, 5.0\n"
                + "5, Emily, 5.5\n"
                + "6, Alice, 6.0\n"
                + "7, Betty, 6.5\n"
                + "8, Alice, 7.0\n"
                + "9, Cindy, 7.5\n"
        );
    }

    @Test
    public void testProjectScan() throws SQLException {
        String sql = "select name as label, amount * 10.0 as score from test";
        sqlHelper.queryTest(
            sql,
            new String[]{"label", "score"},
            TupleSchema.ofTypes("STRING", "DOUBLE"),
            "Alice, 35\n"
                + "Betty, 40\n"
                + "Cindy, 45\n"
                + "Doris, 50\n"
                + "Emily, 55\n"
                + "Alice, 60\n"
                + "Betty, 65\n"
                + "Alice, 70\n"
                + "Cindy, 75\n"
        );
    }

    @Test
    public void testCast() throws SQLException {
        String sql = "select id, name, cast(amount as int) as amount from test";
        sqlHelper.queryTest(
            sql,
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "INTEGER"),
            "1, Alice, 3\n"
                + "2, Betty, 4\n"
                + "3, Cindy, 4\n"
                + "4, Doris, 5\n"
                + "5, Emily, 5\n"
                + "6, Alice, 6\n"
                + "7, Betty, 6\n"
                + "8, Alice, 7\n"
                + "9, Cindy, 7\n"
        );
    }

    @Test
    public void testCaseFun() throws SQLException {
        String sql = "select id, name, case when amount >= 6.0 then 'Y' else 'N' end as flag from test";
        sqlHelper.queryTest(
            sql,
            new String[]{"id", "name", "flag"},
            TupleSchema.ofTypes("INTEGER", "STRING", "STRING"),
            "1, Alice, N\n"
                + "2, Betty, N\n"
                + "3, Cindy, N\n"
                + "4, Doris, N\n"
                + "5, Emily, N\n"
                + "6, Alice, Y\n"
                + "7, Betty, Y\n"
                + "8, Alice, Y\n"
                + "9, Cindy, Y\n"
        );
    }

    @Test
    public void testCaseFun1() throws SQLException {
        String sql = "select id, name, case"
            + " when amount >= 7.0 then 'A' "
            + " when amount >= 6.0 then 'Y'"
            + " else 'N'"
            + " end as flag from test";
        sqlHelper.queryTest(
            sql,
            new String[]{"id", "name", "flag"},
            TupleSchema.ofTypes("INTEGER", "STRING", "STRING"),
            "1, Alice, N\n"
                + "2, Betty, N\n"
                + "3, Cindy, N\n"
                + "4, Doris, N\n"
                + "5, Emily, N\n"
                + "6, Alice, Y\n"
                + "7, Betty, Y\n"
                + "8, Alice, A\n"
                + "9, Cindy, A\n"
        );
    }

    @Test
    public void testCountOfTable() throws SQLException {
        Connection connection = sqlHelper.getConnection();
        String sql = "select count(*) from test";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getInt(1));
                    Assertions.assertEquals(rs.getInt(1), 9);
                }
            }
        }
    }
}
