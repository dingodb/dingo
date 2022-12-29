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
import io.dingodb.test.cases.CasesInFileJUnit5;
import io.dingodb.test.cases.InputTestFile;
import io.dingodb.test.cases.StressTestCases;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

// Before run this, you must set up your cluster.
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DingoDriverClientIT {
    private static SqlHelper sqlHelper;

    public static Connection getConnection() throws ClassNotFoundException, SQLException, IOException {
        Class.forName("io.dingodb.driver.client.DingoDriverClient");
        Properties properties = new Properties();
        properties.load(DingoDriverClientIT.class.getResourceAsStream("/intTest.properties"));
        String url = properties.getProperty("url");
        return DriverManager.getConnection(
            DingoDriverClient.CONNECT_STRING_PREFIX + "url=" + url,
            properties
        );
    }

    @BeforeAll
    public static void setupAll() throws Exception {
        Connection connection = getConnection();
        sqlHelper = new SqlHelper(connection);
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ArgumentsSource(CasesInFileJUnit5.class)
    public void test(String ignored, List<InputTestFile> files) throws SQLException, IOException {
        sqlHelper.randomTable().doTestFiles(files);
    }

    @Test
    public void testParameterQuery() throws SQLException, IOException {
        RandomTable randomTable = sqlHelper.randomTable();
        randomTable.execFiles(
            "string_double/create.sql",
            "string_double/data.sql"
        );
        String sql = "select * from {table} where id < ? and amount > ?";
        try (PreparedStatement statement = randomTable.prepare(sql)) {
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
        randomTable.drop();
    }

    @Test
    public void testInsertWithParameters() throws SQLException, IOException {
        RandomTable randomTable = sqlHelper.randomTable();
        randomTable.execFiles(
            "string_double/create.sql",
            "string_double/data.sql"
        );
        String sql = "insert into {table} values(?, ?, ?)";
        try (PreparedStatement statement = randomTable.prepare(sql)) {
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
        randomTable.queryTest(
            "select * from {table} where id >= 10",
            new String[]{"id", "name", "amount"},
            ImmutableList.of(
                new Object[]{10, "Alice", 10.0},
                new Object[]{11, "Betty", 11.0}
            )
        );
        randomTable.drop();
    }

    @Test
    public void testStressInsert() throws SQLException, IOException {
        StressTestCases.testInsert(sqlHelper);
    }

    @Test
    public void testStressInsertWithParameters() throws SQLException, IOException {
        StressTestCases.testInsertWithParameters(sqlHelper);
    }

    @Test
    public void testStressInsertWithParametersBatch() throws SQLException, IOException {
        StressTestCases.testInsertWithParametersBatch(sqlHelper);
    }
}
