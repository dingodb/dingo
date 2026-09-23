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

import io.dingodb.driver.DingoResultSetMetaData;
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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    public void mysqlCharCoercesDecimalArguments() throws SQLException {
        assertThat(context.querySingleValue("select hex(char(77.6))")).isEqualTo("4E");
        assertThat(context.querySingleValue("select hex(char('77.3'))")).isEqualTo("4D");
        assertThat(context.querySingleValue("select hex(char(77.6 using binary))")).isEqualTo("4E");
        assertThat(context.querySingleValue("select char(77.6 using utf8mb4)")).isEqualTo("N");
    }

    @Test
    public void convertCharsetRetainsEncodedByteSemantics() throws SQLException {
        try (Statement statement = context.getConnection().createStatement();
             ResultSet result = statement.executeQuery("select length(convert('é' using latin1))")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(1);
        }
        assertThat(context.querySingleValue("select hex(convert('€' using latin1))")).isEqualTo("80");
        assertThat(context.querySingleValue("select char(128 using latin1)")).isEqualTo("€");
        assertThat(context.querySingleValue("select length(char(128 using latin1))")).isEqualTo(1);
        assertThat(context.querySingleValue("select hex(char(128 using latin1))")).isEqualTo("80");
        assertThat(context.querySingleValue("select hex(convert('é' using latin1))")).isEqualTo("E9");
        try (Statement statement = context.getConnection().createStatement();
             ResultSet result = statement.executeQuery("select concat(convert('é' using latin1), 'x')")) {
            assertThat(((DingoResultSetMetaData) result.getMetaData()).getColumnCharsetName(1))
                .isEqualTo("windows-1252");
            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).isEqualTo("éx");
        }
        assertThat(context.querySingleValue(
            "select length(concat(convert('é' using latin1), 'x'))"
        )).isEqualTo(2);
        assertThat(context.querySingleValue(
            "select hex(concat(convert('é' using latin1), 'x'))"
        )).isEqualTo("E978");
        try (Statement statement = context.getConnection().createStatement();
             ResultSet result = statement.executeQuery("select convert('é' using latin1)")) {
            assertThat(result.getMetaData().getColumnType(1)).isEqualTo(Types.VARCHAR);
            assertThat(result.getMetaData()).isInstanceOf(DingoResultSetMetaData.class);
            assertThat(((DingoResultSetMetaData) result.getMetaData()).getColumnCharsetName(1))
                .isEqualTo("windows-1252");
            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).isEqualTo("é");
        }
    }

    @Test
    public void concatPromotesMixedCharsetsWithoutLosingCharacters() throws SQLException {
        String latin1 = "convert(char(128 using latin1) using latin1)";
        String unicode = "convert(char(240,159,153,130 using utf8mb4) using utf8mb4)";
        assertThat(context.querySingleValue("select hex(concat(" + latin1 + ", " + unicode + "))"))
            .isEqualTo("E282ACF09F9982");
        assertThat(context.querySingleValue("select hex(concat(" + unicode + ", " + latin1 + "))"))
            .isEqualTo("F09F9982E282AC");
        assertThat(context.querySingleValue("select length(concat(" + latin1 + ", " + unicode + "))"))
            .isEqualTo(7);
    }

    @Test
    public void concatKeepsRepresentableNonUnicodeCharset() throws SQLException {
        String latin1 = "convert(char(128 using latin1) using latin1)";
        String ascii = "convert(char(65 using ascii) using ascii)";
        assertThat(context.querySingleValue("select hex(concat(" + latin1 + ", " + ascii + "))"))
            .isEqualTo("8041");
        assertThat(context.querySingleValue("select hex(concat(" + ascii + ", " + latin1 + "))"))
            .isEqualTo("4180");
    }

    @Test
    public void convertCharsetRejectsUnrepresentableText() {
        assertThatThrownBy(() -> {
            try (Statement statement = context.getConnection().createStatement();
                 ResultSet result = statement.executeQuery("select convert('é' using ascii)")) {
                result.next();
            }
        }).hasMessageContaining("Cannot convert value");
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
    public void convertTzInvalidZoneAdvertisesNullableResult() throws SQLException {
        try (Statement statement = context.getConnection().createStatement();
             ResultSet result = statement.executeQuery(
                 "select convert_tz('2024-01-01 00:00:00', 'invalid-zone', 'UTC')"
             )) {
            assertThat(result.getMetaData().isNullable(1)).isEqualTo(ResultSetMetaData.columnNullable);
            assertThat(result.next()).isTrue();
            assertThat(result.getObject(1)).isNull();
        }
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
