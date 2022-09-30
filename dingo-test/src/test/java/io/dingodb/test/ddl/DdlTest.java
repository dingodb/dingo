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
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import io.dingodb.test.SqlHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        "timestamp, 1970-01-01 00:00:00.000",
        "timestamp, 2022-11-01 11:01:01.000",
    })
    public void testCreateTableTimestampLiteral(@Nonnull String type, String value) throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data " + type + ", primary key(id))",
            "insert into {table} values(1, " + type + "'" + value + "')"
        );
        int typeCode = TypeCode.codeOf(type.toUpperCase());
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(DateTimeUtils.toUtcString((Timestamp) result)).isEqualTo(value);
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
    public void testCreateTableWithIntArray() throws SQLException {
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
    public void testCreateTableWithDoubleArray() throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data double array, primary key(id))",
            "insert into {table} values(1, array[1, 2.1, 3.2])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.DOUBLE);
        assertThat(array.getArray()).isEqualTo(new double[]{1, 2.1, 3.2});
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testCreateTableWithStringArray() throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data varchar array, primary key(id))",
            "insert into {table} values(1, array['1', '2', '3'])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.VARCHAR);
        assertThat(array.getArray()).isEqualTo(new String[]{"1", "2", "3"});
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testCreateTableWithStringArrayContainsNull() {
        AvaticaSqlException exception = assertThrows(AvaticaSqlException.class, () -> {
            sqlHelper.prepareTable(
                "create table {table} (id int, data varchar array, primary key(id))",
                "insert into {table} values(1, array['1', null, '3'])"
            );
        });
        assertThat(exception.getMessage()).contains("NULLs are not allowed");
    }

    @Test
    public void testCreateTableWithStringArrayNull() throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data varchar array, primary key(id))",
            "insert into {table} values(1, null)"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result).isNull();
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testCreateTableWithStringArrayNull1() throws SQLException {
        AvaticaSqlException exception = assertThrows(AvaticaSqlException.class, () -> {
            sqlHelper.prepareTable(
                "create table {table} (id int, data varchar array not null, primary key(id))",
                "insert into {table} values(1, null)"
            );
        });
        assertThat(exception.getMessage()).contains("does not allow NULLs");
    }

    @Test
    public void testCreateTableWithDateArray() throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data date array, primary key(id))",
            "insert into {table} values(1, array['1970-01-01', '1980-2-2', '19900303'])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.DATE);
        List<String> dateStrings = Arrays.stream((Object[]) array.getArray())
            .map(Object::toString)
            .collect(Collectors.toList());
        assertThat(dateStrings).containsExactly("1970-01-01", "1980-02-02", "1990-03-03");
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testCreateTableWithMultiset() throws SQLException {
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
    public void testCreateTableWithMultiset1() throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, name char(8), data int multiset, primary key(id))",
            "insert into {table} values(1, 'ABC', multiset[7, 7, 8, 8])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.INTEGER);
        assertThat(array.getArray()).isEqualTo(new int[]{7, 7, 8, 8});
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testCreateTableWithMultisetDefault() throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} ("
                + "id int,"
                + "name char(8),"
                + "data int multiset default multiset[1, 2, 3],"
                + "primary key(id)"
                + ")",
            "insert into {table}(id, name) values(1, 'ABC')"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.INTEGER);
        assertThat(array.getArray()).isEqualTo(new int[]{1, 2, 3});
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testCreateTableWithDoubleMultiset() throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data double multiset, primary key(id))",
            "insert into {table} values(1, multiset[1, 2.1, 3.2])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.DOUBLE);
        assertThat(array.getArray()).isEqualTo(new double[]{1, 2.1, 3.2});
        sqlHelper.dropTable(tableName);
    }

    @Test
    @Disabled
    public void testCreateTableWithDateMultiset() throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data date multiset, primary key(id))",
            "insert into {table} values(1, multiset['1970-01-01', '1980-2-2', '19900303'])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.DATE);
        List<String> dateStrings = Arrays.stream((Object[]) array.getArray())
            .map(Object::toString)
            .collect(Collectors.toList());
        assertThat(dateStrings).containsExactly("1970-01-01", "1980-02-02", "1990-03-03");
        sqlHelper.dropTable(tableName);
    }

    @Test
    @Disabled
    public void testCreateTableWithMultisetAndUpdate() throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, name char(8), data int multiset, primary key(id))",
            "insert into {table} values(1, 'ABC', multiset[7, 7, 8, 8])"
        );
        sqlHelper.updateTest(
            "update " + tableName + " set data = multiset[1, 2, 3] where id = 1",
            1
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.INTEGER);
        assertThat(array.getArray()).isEqualTo(new int[]{1, 2, 3});
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

    @Test
    public void testCreateTableWithMapNullValue() {
        AvaticaSqlException exception = assertThrows(AvaticaSqlException.class, () -> {
            sqlHelper.prepareTable(
                "create table {table} (id int, data map, primary key(id))",
                "insert into {table} values(1, map['a', '1', 'b', null])"
            );
        });
        assertThat(exception.getMessage()).contains("NULLs are not allowed");
    }

    @Test
    public void testCreateTableWithMapNull() throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data map, primary key(id))",
            "insert into {table} values(1, null)"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result).isNull();
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testCreateTableWithMapNull1() {
        AvaticaSqlException exception = assertThrows(AvaticaSqlException.class, () -> {
            sqlHelper.prepareTable(
                "create table {table} (id int, data map not null, primary key(id))",
                "insert into {table} values(1, null)"
            );
        });
        assertThat(exception.getMessage()).contains("does not allow NULLs");
    }

    @Test
    public void testCreateTableWithMapMixedType() throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} (id int, data map, primary key(id))",
            "insert into {table} values(1, map['a', 1, 'b', 2.5])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result).isInstanceOf(Map.class)
            .isEqualTo(ImmutableMap.of("a", BigDecimal.valueOf(1), "b", BigDecimal.valueOf(2.5)));
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void testCreateTableWithMapDefault() throws SQLException {
        String tableName = sqlHelper.prepareTable(
            "create table {table} ("
                + "id int,"
                + "name char(8),"
                + "data map default map['a', 1, 'b', 2],"
                + "primary key(id)"
                + ")",
            "insert into {table}(id, name) values(1, 'ABC')"
        );
        Object result = sqlHelper.querySingleValue("select data from " + tableName);
        assertThat(result).isInstanceOf(Map.class)
            .isEqualTo(ImmutableMap.of("a", 1, "b", 2));
        sqlHelper.dropTable(tableName);
    }
}
