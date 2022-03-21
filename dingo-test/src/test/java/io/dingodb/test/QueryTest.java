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
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.Services;
import io.dingodb.meta.test.MetaTestService;
import io.dingodb.test.asserts.AssertResultSet;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class QueryTest {
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
    private static Connection connection;
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        Services.metaServices.get(MetaTestService.SCHEMA_NAME).init(null);
        Services.initNetService();
        connection = Connections.getConnection(MetaTestService.SCHEMA_NAME);
        sqlHelper = new SqlHelper(connection);
        sqlHelper.execUpdate("/table-test-create.sql");
        sqlHelper.execUpdate("/table-test1-create.sql");
        //sqlHelper.execUpdate("/table-test2-create.sql");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        connection.close();
        Services.metaServices.get(MetaTestService.SCHEMA_NAME).clear();
    }

    private static void checkDatumInTable(
        String tableName,
        String[] columns,
        TupleSchema schema,
        String data
    ) throws SQLException {
        String sql = "select * from " + tableName;
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(columns, schema, data);
            }
        }
    }

    private static void checkDatumInTestTable(String data) throws SQLException {
        checkDatumInTable("test",
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            data
        );
    }

    @BeforeEach
    public void setup() throws Exception {
        sqlHelper.execUpdate("/table-test-data.sql");
    }

    @AfterEach
    public void cleanUp() throws Exception {
        sqlHelper.clear("test");
    }

    @Test
    public void testGetSchemas() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet = metaData.getSchemas();
        AssertResultSet.of(resultSet).isRecords(
            new String[]{"TABLE_SCHEM", "TABLE_CATALOG"},
            TupleSchema.ofTypes("STRING", "STRING"),
            Arrays.asList(
                new Object[]{"metadata", null},
                new Object[]{"TEST", null}
            )
        );
    }

    @Test
    public void testGetTables() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet = metaData.getTables(null, "TEST", null, null);
        AssertResultSet.of(resultSet).isRecords(
            new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE"},
            TupleSchema.ofTypes("STRING", "STRING", "STRING", "STRING"),
            Arrays.asList(
                new Object[]{null, "TEST", "TEST", "TABLE"},
                new Object[]{null, "TEST", "TEST1", "TABLE"}
            )
        );
    }

    @Test
    public void testGetColumns() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet = metaData.getColumns(null, "TEST", "TEST", null);
        AssertResultSet.of(resultSet).isRecords(
            new String[]{
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                "TYPE_NAME", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
                "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"
            },
            TupleSchema.ofTypes(
                "STRING", "STRING", "STRING", "STRING", "INTEGER",
                "STRING", "INTEGER", "INTEGER", "INTEGER", "INTEGER",
                "INTEGER", "INTEGER", "STRING", "STRING", "STRING"
            ),
            Arrays.asList(
                new Object[]{
                    null, "TEST", "TEST", "ID", 4,
                    "INTEGER NOT NULL", -1, null, 10, 0,
                    -1, 1, "NO", "", ""
                },
                new Object[]{
                    null, "TEST", "TEST", "NAME", 12,
                    "VARCHAR(32) NOT NULL", 32, null, 10, 0,
                    32, 2, "NO", "", ""
                },
                new Object[]{
                    null, "TEST", "TEST", "AMOUNT", 8,
                    "DOUBLE NOT NULL", -1, null, 10, 0,
                    -1, 3, "NO", "", ""
                }
            )
        );
    }

    @Test
    @Disabled
    public void testExplainSimpleValues() throws SQLException {
        String sql = "explain plan for select 1";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isPlan(
                    "EnumerableRoot",
                    "DingoValues"
                );
            }
        }
    }

    @Test
    @Disabled
    public void testExplainInsertValues() throws SQLException {
        String sql = "explain plan for insert into test values(1, 'Alice', 1.0)";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isPlan(
                    "EnumerableRoot",
                    "DingoCoalesce",
                    "DingoExchange",
                    "DingoPartModify",
                    "DingoDistributedValues"
                );
            }
        }
    }

    @Test
    @Disabled
    public void testExplainScan() throws SQLException {
        String sql = "explain plan for select * from dingo.test";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isPlan(
                    "EnumerableRoot",
                    "DingoCoalesce",
                    "DingoExchange",
                    "DingoPartScan"
                );
            }
        }
    }

    @Test
    public void testSimpleValues() throws SQLException {
        // Queries like 'select 1' is bypassed by Calcite.
        String sql = "select 'hello'";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"EXPR$0"},
                    TupleSchema.ofTypes("STRING"),
                    "hello"
                );
            }
        }
    }

    @Test
    public void testScan() throws SQLException, IOException {
        checkDatumInTestTable(TEST_ALL_DATA);
    }

    @Test
    public void testScan1() throws SQLException, IOException {
        checkDatumInTable(
            "test.test",
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            TEST_ALL_DATA
        );
    }

    // TODO: currently the records overlap to each other for the key is empty if there is no primary key.
    //@Test
    public void testScan2() throws SQLException, IOException {
        sqlHelper.execUpdate("/table-test2-data.sql");
        String sql = "select * from test2";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                int count = 0;
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    Double amount = resultSet.getDouble("amount");
                    log.info("id = {}, name = {}, amount = {}.", id, name, amount);
                    ++count;
                }
                assertThat(count).isEqualTo(1);
            }
        }
        sqlHelper.clear("test2");
    }

    @Test
    public void testGetByKey() throws SQLException {
        String sql = "select * from test where id = 1";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"id", "name", "amount"},
                    TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
                    "1, Alice, 3.5\n"
                );
            }
        }
    }

    @Test
    public void testGetByKey1() throws SQLException {
        String sql = "select * from test where id = 1 or id = 2";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"id", "name", "amount"},
                    TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
                    "1, Alice, 3.5\n"
                        + "2, Betty, 4.0\n"
                );
            }
        }
    }

    @Test
    public void testGetByKey2() throws SQLException {
        String sql = "select * from test where id in (1, 2, 3)";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"id", "name", "amount"},
                    TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
                    "1, Alice, 3.5\n"
                        + "2, Betty, 4.0\n"
                        + "3, Cindy, 4.5\n"
                );
            }
        }
    }

    @Test
    public void testFilterScan() throws SQLException {
        String sql = "select * from test where amount > 4.0";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
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
        }
    }

    @Test
    public void testProjectScan() throws SQLException {
        String sql = "select name as label, amount * 10.0 as score from test";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
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
        }
    }

    @Test
    public void testCount() throws SQLException {
        String sql = "select name, count(*) from test group by name";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"name", "expr$1"},
                    TupleSchema.ofTypes("STRING", "LONG"),
                    "Alice, 3\n"
                        + "Betty, 2\n"
                        + "Cindy, 2\n"
                        + "Doris, 1\n"
                        + "Emily, 1\n"
                );
            }
        }
    }

    @Test
    public void testCount1() throws SQLException {
        String sql = "select count(*) from test";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"expr$0"},
                    TupleSchema.ofTypes("LONG"),
                    "9\n"
                );
            }
        }
    }

    @Test
    public void testCount2() throws SQLException {
        String sql = "select count(*) from test group by name";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"expr$0"},
                    TupleSchema.ofTypes("LONG"),
                    "3\n"
                        + "2\n"
                        + "2\n"
                        + "1\n"
                        + "1\n"
                );
            }
        }
    }

    @Test
    public void testSum() throws SQLException {
        String sql = "select name, sum(amount) as total_amount from test group by name";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"name", "total_amount"},
                    TupleSchema.ofTypes("STRING", "DOUBLE"),
                    "Alice, 16.5\n"
                        + "Betty, 10.5\n"
                        + "Cindy, 12.0\n"
                        + "Doris, 5.0\n"
                        + "Emily, 5.5\n"
                );
            }
        }
    }

    @Test
    public void testSum1() throws SQLException {
        String sql = "select sum(amount) as all_sum from test";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"all_sum"},
                    TupleSchema.ofTypes("DOUBLE"),
                    "49.5\n"
                );
            }
        }
    }

    @Test
    public void testMin() throws SQLException {
        String sql = "select min(amount) as min_amount from test";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"min_amount"},
                    TupleSchema.ofTypes("DOUBLE"),
                    "3.5\n"
                );
            }
        }
    }

    @Test
    public void testMax() throws SQLException {
        String sql = "select max(amount) as max_amount from test";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"max_amount"},
                    TupleSchema.ofTypes("DOUBLE"),
                    "7.5\n"
                );
            }
        }
    }

    @Test
    public void testAvg() throws SQLException {
        String sql = "select avg(amount) as avg_amount from test";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"avg_amount"},
                    TupleSchema.ofTypes("DOUBLE"),
                    "5.5\n"
                );
            }
        }
    }

    @Test
    public void testAvg1() throws SQLException {
        String sql = "select name, avg(amount) as avg_amount from test group by name";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"name", "avg_amount"},
                    TupleSchema.ofTypes("STRING", "DOUBLE"),
                    "Alice, 5.5\n"
                        + "Betty, 5.25\n"
                        + "Cindy, 6.0\n"
                        + "Doris, 5.0\n"
                        + "Emily, 5.5"
                );
            }
        }
    }

    @Test
    public void testAvg2() throws SQLException {
        String sql = "select name, avg(id) as avg_id, avg(amount) as avg_amount from test group by name";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"name", "avg_id", "avg_amount"},
                    TupleSchema.ofTypes("STRING", "LONG", "DOUBLE"),
                    "Alice, 5, 5.5\n"
                        + "Betty, 4, 5.25\n"
                        + "Cindy, 6, 6.0\n"
                        + "Doris, 4, 5.0\n"
                        + "Emily, 5, 5.5"
                );
            }
        }
    }

    @Test
    public void testCast() throws SQLException {
        String sql = "select id, name, cast(amount as int) as amount from test";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"id", "name", "amount"},
                    TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
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
        }
    }

    @Test
    public void testSort() throws SQLException {
        String sql = "select * from test order by id asc";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecordsInOrder(
                    new String[]{"id", "name", "amount"},
                    TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
                    TEST_ALL_DATA
                );
            }
        }
    }

    @Test
    public void testSort1() throws SQLException {
        String sql = "select * from test order by name desc, amount";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecordsInOrder(
                    new String[]{"id", "name", "amount"},
                    TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
                    "5, Emily, 5.5\n"
                        + "4, Doris, 5.0\n"
                        + "3, Cindy, 4.5\n"
                        + "9, Cindy, 7.5\n"
                        + "2, Betty, 4.0\n"
                        + "7, Betty, 6.5\n"
                        + "1, Alice, 3.5\n"
                        + "6, Alice, 6.0\n"
                        + "8, Alice, 7.0\n"
                );
            }
        }
    }

    @Test
    public void testSortLimitOffset() throws SQLException {
        String sql = "select * from test order by name desc, amount limit 3 offset 2";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecordsInOrder(
                    new String[]{"id", "name", "amount"},
                    TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
                    "3, Cindy, 4.5\n"
                        + "9, Cindy, 7.5\n"
                        + "2, Betty, 4.0\n"
                );
            }
        }
    }

    @Test
    public void testUpdate() throws SQLException {
        String sql = "update test set amount = 100 where id = 1";
        try (Statement statement = connection.createStatement()) {
            int count = statement.executeUpdate(sql);
            assertThat(count).isEqualTo(1);
        }
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
        try (Statement statement = connection.createStatement()) {
            int count = statement.executeUpdate(sql);
            assertThat(count).isEqualTo(9);
        }
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
        try (Statement statement = connection.createStatement()) {
            int count = statement.executeUpdate(sql);
            assertThat(count).isEqualTo(2);
        }
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
        try (Statement statement = connection.createStatement()) {
            int count = statement.executeUpdate(sql);
            assertThat(count).isEqualTo(3);
        }
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
        try (Statement statement = connection.createStatement()) {
            int count = statement.executeUpdate(sql);
            assertThat(count).isEqualTo(2);
        }
        checkDatumInTestTable(
            TEST_ALL_DATA
                + "10, Alice, 8.0\n"
                + "11, Cindy, 8.5\n"
        );
    }

    @Test
    public void testTransfer() throws SQLException {
        String sql = "insert into test1 select id, name, amount > 6.0, name, amount+1.0 from test where amount > 5.0";
        try (Statement statement = connection.createStatement()) {
            int count = statement.executeUpdate(sql);
            assertThat(count).isEqualTo(5);
        }
        checkDatumInTable(
            "test1",
            new String[]{"id0", "id1", "id2", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "BOOLEAN", "STRING", "DOUBLE"),
            "5, Emily, false, Emily, 6.5\n"
                + "6, Alice, false, Alice, 7.0\n"
                + "7, Betty, true, Betty, 7.5\n"
                + "8, Alice, true, Alice, 8.0\n"
                + "9, Cindy, true, Cindy, 8.5\n"
        );
    }
}
