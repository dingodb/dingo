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

package io.dingodb.test.exception;

import com.google.common.collect.ImmutableList;
import io.dingodb.test.RandomTable;
import io.dingodb.test.SqlHelper;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Slf4j
public class ExceptionTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @NonNull
    public static Stream<Arguments> getParameters() {
        return Stream.of(
            // Parsing error
            arguments("SQL Parse error", ImmutableList.of(
                "select"
            ), 51001, "51001", false),
            arguments("Illegal expression in context", ImmutableList.of(
                "insert into {table} (1)"
            ), 51002, "51002", false),
            // DDL error
            arguments("Unknown identifier", ImmutableList.of(
                "create table {table} (id int, data bomb, primary key(id))"
            ), 52001, "52001", false),
            arguments("Table already exists", ImmutableList.of(
                "create table {table} (id int, primary key(id))",
                "create table {table} (id int, primary key(id))"
            ), 52002, "52002", true),
            arguments("Create without primary key", ImmutableList.of(
                "create table {table} (id int)"
            ), 52003, "52003", false),
            arguments("Missing column list", ImmutableList.of(
                "create table {table}"
            ), 52004, "52004", false),
            // validation error
            arguments("Table not found", ImmutableList.of(
                "select * from {table}"
            ), 53001, "53001", false),
            arguments("Column not found", ImmutableList.of(
                "create table {table} (id int, primary key(id))",
                "select data from {table}"
            ), 53002, "53002", true),
            arguments("Column not allow null", ImmutableList.of(
                "create table {table} (id int, primary key(id))",
                "insert into {table} values(null)"
            ), 53003, "53003", true),
            arguments("Column not allow null (array)", ImmutableList.of(
                "create table {table} (id int, data varchar array not null, primary key(id))",
                "insert into {table} values(1, null)"
            ), 53003, "53003", true),
            arguments("Column not allow null (map)", ImmutableList.of(
                "create table {table} (id int, data map not null, primary key(id))",
                "insert into {table} values(1, null)"
            ), 53003, "53003", true),
            // execution error
            arguments("Task failed", ImmutableList.of(
                "create table {table} (id int, data double, primary key(id))",
                "insert into {table} values (1, 3.5)",
                "update {table} set data = 'abc'"
            ), 60000, "60000", true)
        );
    }

    @NonNull
    public static Stream<Arguments> getParametersTemp() {
        return Stream.of(
            arguments("Missing column list", ImmutableList.of(
                "create table {table} (id int, data int, primary key(id))",
                "insert into {table} values(1, 'abc')"
            ), 53004, "53004", false)
        );
    }

    private static @NonNull SQLException getException(List<String> sqlList, boolean needDropping) {
        RandomTable randomTable = sqlHelper.randomTable();
        SQLException exception = assertThrows(SQLException.class, () -> {
            randomTable.execSqls(sqlList.toArray(new String[0]));
        });
        if (needDropping) {
            try {
                randomTable.drop();
            } catch (SQLException ignored) {
            }
        }
        log.info("Exception = {}", exception.getMessage());
        return exception;
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("getParameters")
    public void testException(
        String ignored,
        @NonNull List<String> sqlList,
        int sqlCode,
        String sqlState,
        boolean needDropping
    ) {
        SQLException exception = getException(sqlList, needDropping);
        assertThat(exception.getErrorCode()).isEqualTo(sqlCode);
        assertThat(exception.getSQLState()).isEqualTo(sqlState);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("getParametersTemp")
    public void testExceptionTemp(
        String testName,
        @NonNull List<String> sqlList,
        int sqlCode,
        String sqlState,
        boolean needDropping
    ) {
        testException(testName, sqlList, sqlCode, sqlState, needDropping);
    }
}
