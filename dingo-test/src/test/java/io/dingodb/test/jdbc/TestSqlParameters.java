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

package io.dingodb.test.jdbc;

import com.google.common.collect.ImmutableList;
import io.dingodb.test.SqlHelper;
import io.dingodb.test.asserts.AssertResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestSqlParameters {
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

    @Test
    public void testIllegalUse() throws SQLException {
        String sql = "select ?";
        SQLException e = assertThrows(SQLException.class, () -> {
            try (PreparedStatement statement = sqlHelper.getConnection().prepareStatement(sql)) {
                statement.setInt(1, 1);
                try (ResultSet resultSet = statement.executeQuery()) {
                    AssertResultSet.of(resultSet).isRecords(ImmutableList.of(
                        new Object[]{1}
                    ));
                }
            }
        });
        assertThat(e.getCause().getMessage()).contains("Illegal use of dynamic parameter");
    }

    @Test
    public void testSimple() throws SQLException {
        String sql = "select 1 + ?";
        try (PreparedStatement statement = sqlHelper.getConnection().prepareStatement(sql)) {
            statement.setInt(1, 1);
            try (ResultSet resultSet = statement.executeQuery()) {
                AssertResultSet.of(resultSet).isRecords(ImmutableList.of(
                    new Object[]{2}
                ));
            }
            statement.setInt(1, 2);
            try (ResultSet resultSet = statement.executeQuery()) {
                AssertResultSet.of(resultSet).isRecords(ImmutableList.of(
                    new Object[]{3}
                ));
            }
        }
    }

    @Test
    public void testFilter() throws SQLException {
        String sql = "select * from test where id < ? and amount > ?";
        Connection connection = sqlHelper.getConnection();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, 8);
            statement.setDouble(2, 5.0);
            try (ResultSet resultSet = statement.executeQuery()) {
                AssertResultSet.of(resultSet).isRecords(ImmutableList.of(
                    new Object[]{5, "Emily", 5.5},
                    new Object[]{6, "Alice", 6.0},
                    new Object[]{7, "Betty", 6.5}
                ));
            }
            statement.setDouble(2, 6.0);
            try (ResultSet resultSet = statement.executeQuery()) {
                AssertResultSet.of(resultSet).isRecords(ImmutableList.of(
                    new Object[]{7, "Betty", 6.5}
                ));
            }
        }
    }

    @Test
    public void testInsert() throws SQLException {
        String sql = "insert into test values(?, ?, ?)";
        Connection connection = sqlHelper.getConnection();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, 10);
            statement.setString(2, "Alice");
            statement.setDouble(3, 10.0);
            int count = statement.executeUpdate();
            assertThat(count).isEqualTo(1);
            statement.setInt(1, 11);
            statement.setString(2, "Betty");
            statement.setDouble(3, 11.0);
            count = statement.executeUpdate();
            assertThat(count).isEqualTo(1);
        }
        sqlHelper.queryTest(
            "select * from test where id >= 10",
            new String[]{"id", "name", "amount"},
            ImmutableList.of(
                new Object[]{10, "Alice", 10.0},
                new Object[]{11, "Betty", 11.0}
            )
        );
    }
}
