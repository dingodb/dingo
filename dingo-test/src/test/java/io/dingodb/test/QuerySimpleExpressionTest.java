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

import io.dingodb.test.asserts.Assert;
import io.dingodb.test.cases.RexCasesJUnit5;
import io.dingodb.test.dsl.run.exec.SqlExecContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.sql.SQLException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class QuerySimpleExpressionTest {
    private SqlExecContext context;

    @BeforeAll
    public static void setupAll() throws Exception {
        ConnectionFactory.initLocalEnvironment();
    }

    @AfterAll
    public static void cleanUpAll() {
        ConnectionFactory.cleanUp();
    }

    @BeforeEach
    public void setup() throws Exception {
        context = new SqlExecContext(ConnectionFactory.getConnection());
    }

    @AfterEach
    public void cleanUp() throws Exception {
        context.cleanUp();
    }

    @ParameterizedTest
    @ArgumentsSource(RexCasesJUnit5.class)
    public void test(String sql, String ignored, Object value) throws SQLException {
        Object result = context.querySingleValue("select " + sql);
        Assert.of(result).isEqualTo(value);
    }
}
