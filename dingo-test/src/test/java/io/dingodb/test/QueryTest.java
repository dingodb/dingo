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
import io.dingodb.net.netty.NetServiceConfiguration;
import io.dingodb.test.assertion.AssertResultSet;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.net.DatagramSocket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@TestMethodOrder(CustomTestOrder.class)
public class QueryTest {
    private static Connection connection;

    @BeforeAll
    public static void setupAll() throws Exception {
        Services.META.init(null);
        DatagramSocket tempSocket = new DatagramSocket();
        Services.NET.listenPort(tempSocket.getLocalPort());
        NetServiceConfiguration.instance().setInt(NetServiceConfiguration.PORT_KEY, tempSocket.getLocalPort());
        tempSocket.close();
        Services.initNetService();
        SqlHelper.execUpdate("/table-test-create.sql");
        SqlHelper.execUpdate("/table-test1-create.sql");
        SqlHelper.execUpdate("/table-test2-create.sql");
        SqlHelper.execUpdate("/table-test-data.sql");
        SqlHelper.execUpdate("/table-test2-data.sql");
        connection = Connections.getConnection();
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        connection.close();
        Services.META.clear();
    }

    private static void checkDatumInTestTable(String data) throws SQLException {
        String sql = "select * from test";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(
                    new String[]{"id", "name", "amount"},
                    TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
                    data
                );
            }
        }
    }

    @Test
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
    public void testScan() throws SQLException {
        checkDatumInTestTable(
            "1, Alice, 3.5\n"
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

    // TODO: currently the records overlap to each other for the key is empty if there is no primary key.
    @Test
    public void testScan2() throws SQLException {
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
    public void testUpdate() throws SQLException {
        String sql = "update test set amount = 100 where id = 1";
        try (Statement statement = connection.createStatement()) {
            int count = statement.executeUpdate(sql);
            assertThat(count).isEqualTo(1);
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
    }

    @Test
    public void testUpdate1() throws SQLException {
        String sql = "update test set amount = amount + 100";
        try (Statement statement = connection.createStatement()) {
            int count = statement.executeUpdate(sql);
            assertThat(count).isEqualTo(9);
            checkDatumInTestTable(
                "1, Alice, 200.0\n"
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
    }

    @Test
    public void testDelete() throws SQLException {
        String sql = "delete from test where id = 3 or id = 4";
        try (Statement statement = connection.createStatement()) {
            int count = statement.executeUpdate(sql);
            assertThat(count).isEqualTo(2);
            checkDatumInTestTable(
                "1, Alice, 200.0\n"
                    + "2, Betty, 104.0\n"
                    + "5, Emily, 105.5\n"
                    + "6, Alice, 106.0\n"
                    + "7, Betty, 106.5\n"
                    + "8, Alice, 107.0\n"
                    + "9, Cindy, 107.5\n"
            );
        }
    }

    @Test
    public void testDelete1() throws SQLException {
        String sql = "delete from test where name = 'Alice'";
        try (Statement statement = connection.createStatement()) {
            int count = statement.executeUpdate(sql);
            assertThat(count).isEqualTo(3);
            checkDatumInTestTable(
                "2, Betty, 104.0\n"
                    + "5, Emily, 105.5\n"
                    + "7, Betty, 106.5\n"
                    + "9, Cindy, 107.5\n"
            );
        }
    }
}
