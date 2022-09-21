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

import com.google.common.collect.ImmutableList;
import io.dingodb.driver.client.DingoDriverClient;
import io.dingodb.test.asserts.Assert;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

// Before run this, you must set up your cluster.
@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DingoDriverClientIT {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        Class.forName("io.dingodb.driver.client.DingoDriverClient");
        Properties properties = new Properties();
        properties.setProperty("defaultSchema", MetaTestService.SCHEMA_NAME);
        TimeZone timeZone = TimeZone.getDefault();
        properties.setProperty("timeZone", timeZone.getID());
        Connection connection = DriverManager.getConnection(
            DingoDriverClient.CONNECT_STRING_PREFIX + "url=server:8765",
            properties
        );
        sqlHelper = new SqlHelper(connection);
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @Order(1)
    @Test
    public void testCreateTable() throws SQLException, IOException {
        sqlHelper.execFile("/table-test-create.sql");
    }

    @Order(2)
    @Test
    public void testInsertData() throws SQLException, IOException {
        sqlHelper.execFile("/table-test-data.sql");
    }

    @Order(3)
    @Test
    public void testQuery() throws SQLException {
        String sql = "select * from test where id < 8 and amount > 5.0";
        Connection connection = sqlHelper.getConnection();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                Assert.resultSet(resultSet).isRecords(ImmutableList.of(
                    new Object[]{5, "Emily", 5.5},
                    new Object[]{6, "Alice", 6.0},
                    new Object[]{7, "Betty", 6.5}
                ));
            }
        }
    }

    @Order(4)
    @Test
    public void testParameterQuery() throws SQLException {
        String sql = "select * from test where id < ? and amount > ?";
        Connection connection = sqlHelper.getConnection();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, 8);
            statement.setDouble(2, 5.0);
            try (ResultSet resultSet = statement.executeQuery()) {
                Assert.resultSet(resultSet).isRecords(ImmutableList.of(
                    new Object[]{5, "Emily", 5.5},
                    new Object[]{6, "Alice", 6.0},
                    new Object[]{7, "Betty", 6.5}
                ));
            }
            statement.setDouble(2, 6.0);
            try (ResultSet resultSet = statement.executeQuery()) {
                Assert.resultSet(resultSet).isRecords(ImmutableList.of(
                    new Object[]{7, "Betty", 6.5}
                ));
            }
        }
    }

    @Order(5)
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

    @Order(6)
    @Test
    public void testDropTable() throws SQLException {
        sqlHelper.dropTable("test");
    }
}
