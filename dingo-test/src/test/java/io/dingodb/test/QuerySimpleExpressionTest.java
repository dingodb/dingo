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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.sql.SQLException;
import java.sql.Timestamp;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    public void mysqlBinaryAndQuoteKeepLiteralBytes() throws SQLException {
        assertThat(context.querySingleValue("select hex(char(256 using binary))")).isEqualTo("0100");
        assertThat(context.querySingleValue("select quote(convert('x' using binary))")).isEqualTo("'x'");
        assertThat(context.querySingleValue(
            "select hex(quote(char(0, 26, 39, 92 using binary)))"
        )).isEqualTo("275C305C5A5C275C5C27");
        assertThat(context.querySingleValue("select hex(quote(null))")).isEqualTo("4E554C4C");
    }

    @Test
    public void convertTzHandlesFractionalSecondsAndInvalidZones() throws SQLException {
        assertThat(context.querySingleValue(
            "select convert_tz('2024-01-01 12:00:00.123', '+00:00', '+01:00')"
        )).isEqualTo(Timestamp.valueOf("2024-01-01 13:00:00.123"));
        assertThat(context.querySingleValue(
            "select convert_tz('2024-01-01 12:00:00', '', '+01:00')"
        )).isNull();
    }

    @Test
    public void setExpressionUsesGlobalScopeAndRejectsUnsupportedCalls() throws SQLException {
        String globalMode = (String) context.querySingleValue("select @@GLOBAL.sql_mode");
        assertThat(context.execSql("set session sql_mode = 'ANSI'").getException()).isNull();
        assertThat(context.execSql(
            "set session sql_mode = concat(@@GLOBAL.sql_mode, ',STRICT_TRANS_TABLES')"
        ).getException()).isNull();
        String expected = globalMode + ",STRICT_TRANS_TABLES";
        Exception failure = context.execSql(
            "set session sql_mode = dummy_func(@@sql_mode)"
        ).getException();
        assertThat(failure).isNotNull().hasMessageContaining("Unsupported SET expression");
        assertThat(context.getConnection().getClientInfo("sql_mode")).isEqualTo(expected);
    }
}
