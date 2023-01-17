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

import io.dingodb.driver.client.DingoDriverClient;
import io.dingodb.test.asserts.Assert;
import io.dingodb.test.cases.Case;
import io.dingodb.test.cases.CasesProvider;
import io.dingodb.test.cases.ClassTestMethod;
import io.dingodb.test.cases.ExceptionCasesJUnit5;
import io.dingodb.test.cases.ParametersCasesJUnit5;
import io.dingodb.test.cases.RexCasesJUnit5;
import io.dingodb.test.cases.StressCasesJUnit5;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

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

    @ParameterizedTest
    @ArgumentsSource(RexCasesJUnit5.class)
    public void testSqlExpression(String sqlExpression, String ignored, Object value) throws SQLException {
        Object result = sqlHelper.querySingleValue("select " + sqlExpression);
        Assert.of(result).isEqualTo(value);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ArgumentsSource(CasesProvider.class)
    public void test(String ignored, @NonNull Case testCase) throws Exception {
        testCase.run(sqlHelper.getConnection());
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ArgumentsSource(ExceptionCasesJUnit5.class)
    public void testException(
        String ignored,
        @NonNull List<String> sqlList,
        int sqlCode,
        String sqlState,
        boolean needDropping
    ) {
        sqlHelper.exceptionTest(sqlList, needDropping, sqlCode, sqlState);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ArgumentsSource(ParametersCasesJUnit5.class)
    public void testParameters(String ignored, @NonNull ClassTestMethod method) throws Exception {
        method.getMethod().run(sqlHelper);
    }

    @Test
    public void testStressInsert() throws SQLException, IOException {
        new StressCasesJUnit5().insert(sqlHelper);
    }

    @Test
    public void testStressInsertWithParameters() throws SQLException, IOException {
        new StressCasesJUnit5().insertWithParameters(sqlHelper);
    }

    @Test
    public void testStressInsertWithParametersBatch() throws SQLException, IOException {
        new StressCasesJUnit5().insertWithParametersBatch(sqlHelper);
    }
}
