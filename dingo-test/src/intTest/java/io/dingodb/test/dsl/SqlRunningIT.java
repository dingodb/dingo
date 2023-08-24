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

package io.dingodb.test.dsl;

import io.dingodb.driver.client.DingoDriverClient;
import io.dingodb.test.dsl.run.SqlTestRunner;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.stream.Stream;

// Before run this, you must set up your cluster.
public class SqlRunningIT extends SqlTestRunner {
    private static Connection connection;

    public Connection getConnection() throws Exception {
        Class.forName("io.dingodb.driver.client.DingoDriverClient");
        Properties properties = new Properties();
        properties.load(SqlRunningIT.class.getResourceAsStream("/intTest.properties"));
        String url = properties.getProperty("url");
        return DriverManager.getConnection(
            DingoDriverClient.CONNECT_STRING_PREFIX + "url=" + url,
            properties
        );
    }

    @TestFactory
    public Stream<DynamicTest> testStressDml() {
        return getTests(new StressDmlCases());
    }
}
