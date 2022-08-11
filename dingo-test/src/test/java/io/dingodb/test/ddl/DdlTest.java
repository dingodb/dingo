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

package io.dingodb.test.ddl;

import com.google.common.collect.ImmutableMap;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.test.SqlHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Array;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DdlTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @BeforeEach
    public void setup() throws Exception {
    }

    @AfterEach
    public void cleanUp() throws Exception {
    }

    @ParameterizedTest
    @CsvSource({
        "int, 100",
        "bigint, 10000",
        "boolean, true",
        "float, 3.5",
        "double, 2.7",
    })
    public void testCreateTable(@Nonnull String type, String value) throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data " + type + ", primary key(id))",
            "insert into {table} values(1, " + value + ")"
        );
        int typeCode = TypeCode.codeOf(type.toUpperCase());
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        Object expected = DingoTypeFactory.scalar(typeCode, false).parse(value);
        assertThat(result).isEqualTo(expected);
        sqlHelper.dropTable(tableName);
    }

    @ParameterizedTest
    @CsvSource({
        "char, abc",
        "varchar, def",
        "timestamp, 1970-01-01 00:00:00",
        "timestamp, 2022-11-01 11:01:01",
        "binary, abc",
    })
    public void testCreateTableStringLiteral(@Nonnull String type, String value) throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data " + type + ", primary key(id))",
            "insert into {table} values(1, '" + value + "')"
        );
        int typeCode = TypeCode.codeOf(type.toUpperCase());
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        Object expected = DingoTypeFactory.scalar(typeCode, false).parse(value);
        assertThat(result).isEqualTo(expected);
        sqlHelper.dropTable(tableName);
    }

    @ParameterizedTest
    @CsvSource({
        "timestamp, 1970-01-01 00:00:00",
        "timestamp, 2022-11-01 11:01:01",
    })
    public void testCreateTableTimestampLiteral(@Nonnull String type, String value) throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data " + type + ", primary key(id))",
            "insert into {table} values(1, " + type + "'" + value + "')"
        );
        int typeCode = TypeCode.codeOf(type.toUpperCase());
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        Object expected = DingoTypeFactory.scalar(typeCode, false).parse(value);
        assertThat(result).isEqualTo(expected);
        sqlHelper.dropTable(tableName);
    }

    @ParameterizedTest
    @CsvSource({
        "date, 1970-01-01",
        "date, 2022-11-01",
        "time, 00:00:00",
        "time, 04:30:02",
    })
    public void testCreateTableDateTimeCastString(@Nonnull String type, String value) throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data " + type + ", primary key(id))",
            "insert into {table} values(1, '" + value + "')"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result.toString()).isEqualTo(value);
        sqlHelper.dropTable(tableName);
    }

    @ParameterizedTest
    @CsvSource({
        "date, 1970-01-01",
        "date, 2022-11-01",
        "time, 00:00:00",
        "time, 04:30:02",
    })
    public void testCreateTableDateTimeLiteral(@Nonnull String type, String value) throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data " + type + ", primary key(id))",
            "insert into {table} values(1, " + type + "'" + value + "')"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result.toString()).isEqualTo(value);
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testCreateTableWithArray() throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data int array, primary key(id))",
            "insert into {table} values(1, array[1, 2, 3])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.INTEGER);
        assertThat(array.getArray()).isEqualTo(new int[]{1, 2, 3});
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testCreateTableWithMultiSet() throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data int multiset, primary key(id))",
            "insert into {table} values(1, multiset[7, 7, 8, 8])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.INTEGER);
        assertThat(array.getArray()).isEqualTo(new int[]{7, 7, 8, 8});
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testCreateTableWithMap() throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data map, primary key(id))",
            "insert into {table} values(1, map['a', 1, 'b', 2])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result).isInstanceOf(Map.class)
            .isEqualTo(ImmutableMap.of("a", 1, "b", 2));
        sqlHelper.dropTable(tableName);
    }
}
