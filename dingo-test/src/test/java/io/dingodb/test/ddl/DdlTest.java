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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import io.dingodb.test.SqlHelper;
import io.dingodb.test.SqlHelper.RandomTable;
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
import java.sql.Date;
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
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data " + type + ", primary key(id))",
            "insert into {table} values(1, " + value + ")"
        );
        int typeCode = TypeCode.codeOf(type.toUpperCase());
        Object result = sqlHelper.querySingleValue("select data from " + table);
        Object expected = DingoTypeFactory.scalar(typeCode, false).parse(value);
        assertThat(result).isEqualTo(expected);
        table.drop();
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
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data " + type + ", primary key(id))",
            "insert into {table} values(1, '" + value + "')"
        );
        int typeCode = TypeCode.codeOf(type.toUpperCase());
        Object result = sqlHelper.querySingleValue("select data from " + table);
        Object expected = DingoTypeFactory.scalar(typeCode, false).parse(value);
        assertThat(result).isEqualTo(expected);
        table.drop();
    }

    @ParameterizedTest
    @CsvSource({
        "timestamp, 1970-01-01 00:00:00.000",
        "timestamp, 2022-11-01 11:01:01.000",
    })
    public void testCreateTableTimestampLiteral(@Nonnull String type, String value) throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data " + type + ", primary key(id))",
            "insert into {table} values(1, " + type + "'" + value + "')"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(DateTimeUtils.toUtcString(((Timestamp) result).getTime())).isEqualTo(value);
        table.drop();
    }

    @ParameterizedTest
    @CsvSource({
        "date, 1970-01-01",
        "date, 2022-11-01",
        "time, 00:00:00",
        "time, 04:30:02",
    })
    public void testCreateTableDateTimeCastString(@Nonnull String type, String value) throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data " + type + ", primary key(id))",
            "insert into {table} values(1, '" + value + "')"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result.toString()).isEqualTo(value);
        table.drop();
    }

    @ParameterizedTest
    @CsvSource({
        "date, 1970-01-01",
        "date, 2022-11-01",
        "time, 00:00:00",
        "time, 04:30:02",
    })
    public void testCreateTableDateTimeLiteral(@Nonnull String type, String value) throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data " + type + ", primary key(id))",
            "insert into {table} values(1, " + type + "'" + value + "')"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result.toString()).isEqualTo(value);
        table.drop();
    }

    @Test
    public void testCreateTableWithIntArray() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data int array, primary key(id))",
            "insert into {table} values(1, array[1, 2, 3])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.INTEGER);
        assertThat(array.getArray()).isEqualTo(new int[]{1, 2, 3});
        table.drop();
    }

    @Test
    public void testCreateTableWithDoubleArray() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data double array, primary key(id))",
            "insert into {table} values(1, array[1, 2.1, 3.2])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.DOUBLE);
        assertThat(array.getArray()).isEqualTo(new double[]{1, 2.1, 3.2});
        table.drop();
    }

    @Test
    public void testCreateTableWithStringArray() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data varchar array, primary key(id))",
            "insert into {table} values(1, array['1', '2', '3'])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.VARCHAR);
        assertThat(array.getArray()).isEqualTo(new String[]{"1", "2", "3"});
        table.drop();
    }

    @Test
    public void testCreateTableWithStringArrayContainsNull() {
        SQLException exception = assertThrows(SQLException.class, () -> {
            sqlHelper.randomTable().execSqls(
                "create table {table} (id int, data varchar array, primary key(id))",
                "insert into {table} values(1, array['1', null, '3'])"
            );
        });
        assertThat(exception.getMessage()).startsWith("Null values are not allowed");
    }

    @Test
    public void testCreateTableWithStringArrayNull() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data varchar array, primary key(id))",
            "insert into {table} values(1, null)"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isNull();
        table.drop();
    }

    @Test
    public void testCreateTableWithDateArray() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data date array, primary key(id))",
            "insert into {table} values(1, array['1970-01-01', '1980-2-2', '19900303'])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.DATE);
        List<String> dateStrings = Arrays.stream((Object[]) array.getArray())
            .map(Object::toString)
            .collect(Collectors.toList());
        assertThat(dateStrings).containsExactly("1970-01-01", "1980-02-02", "1990-03-03");
        table.drop();
    }

    @Test
    public void testCreateTableWithTimestampArray() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data timestamp array, primary key(id))",
            "insert into {table} values(1, array[1, 2])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.TIMESTAMP);
        assertThat((Object[]) array.getArray()).containsExactly(
            new Timestamp(DateTimeUtils.fromSecond(1)),
            new Timestamp(DateTimeUtils.fromSecond(2))
        );
        table.drop();
        for (Object v : (Object[]) array.getArray()) {
            System.out.println(v + ", ");
        }
    }

    @Test
    public void testCreateTableWithMultiset() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data int multiset, primary key(id))",
            "insert into {table} values(1, multiset[7, 7, 8, 8])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.INTEGER);
        assertThat(array.getArray()).isEqualTo(new int[]{7, 7, 8, 8});
        table.drop();
    }

    @Test
    public void testCreateTableWithMultiset1() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, name char(8), data int multiset, primary key(id))",
            "insert into {table} values(1, 'ABC', multiset[7, 7, 8, 8])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.INTEGER);
        assertThat(array.getArray()).isEqualTo(new int[]{7, 7, 8, 8});
        table.drop();
    }

    @Test
    public void testCreateTableWithMultisetDefault() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} ("
                + "id int,"
                + "name char(8),"
                + "data int multiset default multiset[1, 2, 3],"
                + "primary key(id)"
                + ")",
            "insert into {table}(id, name) values(1, 'ABC')"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.INTEGER);
        assertThat(array.getArray()).isEqualTo(new int[]{1, 2, 3});
        table.drop();
    }

    @Test
    public void testCreateTableWithMultisetDefault1() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} ("
                + "id int,"
                + "name char(8),"
                + "data date multiset default multiset['1970-01-01', '1970-01-02'],"
                + "primary key(id)"
                + ")",
            "insert into {table}(id, name) values(1, 'ABC')"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.DATE);
        assertThat(array.getArray()).isEqualTo(new Date[]{new Date(0), new Date(86400000)});
        table.drop();
    }

    @Test
    public void testCreateTableWithDoubleMultiset() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data double multiset, primary key(id))",
            "insert into {table} values(1, multiset[1, 2.1, 3.2])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.DOUBLE);
        assertThat(array.getArray()).isEqualTo(new double[]{1, 2.1, 3.2});
        table.drop();
    }

    @Test
    public void testCreateTableWithDateMultisetNull() {
        SQLException exception = assertThrows(SQLException.class, () -> {
            sqlHelper.randomTable().execSqls(
                "create table {table} (id int, data date multiset, primary key(id))",
                "insert into {table} values(1, multiset[''])"
            );
        });
        assertThat(exception.getMessage()).startsWith("Null values are not allowed");
    }

    @Test
    @Disabled
    public void testCreateTableWithDateMultiset1() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data date multiset, primary key(id))",
            "insert into {table} values(1, multiset['1970-01-01', '1980-2-2', '19900303'])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.DATE);
        List<String> dateStrings = Arrays.stream((Object[]) array.getArray())
            .map(Object::toString)
            .collect(Collectors.toList());
        assertThat(dateStrings).containsExactly("1970-01-01", "1980-02-02", "1990-03-03");
        table.drop();
    }

    @Test
    @Disabled
    public void testCreateTableWithMultisetAndUpdate() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, name char(8), data int multiset, primary key(id))",
            "insert into {table} values(1, 'ABC', multiset[7, 7, 8, 8])"
        );
        sqlHelper.updateTest(
            "update " + table + " set data = multiset[1, 2, 3] where id = 1",
            1
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isInstanceOf(Array.class);
        Array array = (Array) result;
        assertThat(array.getBaseType()).isEqualTo(Types.INTEGER);
        assertThat(array.getArray()).isEqualTo(new int[]{1, 2, 3});
        table.drop();
    }

    @Test
    public void testCreateTableWithMap() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data map, primary key(id))",
            "insert into {table} values(1, map['a', 1, 'b', 2])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isInstanceOf(Map.class)
            .isEqualTo(ImmutableMap.of("a", 1, "b", 2));
        table.drop();
    }

    @Test
    public void testCreateTableWithMapNullValue() {
        SQLException exception = assertThrows(SQLException.class, () -> {
            sqlHelper.randomTable().execSqls(
                "create table {table} (id int, data map, primary key(id))",
                "insert into {table} values(1, map['a', '1', 'b', null])"
            );
        });
        assertThat(exception.getMessage()).startsWith("Null values are not allowed");
    }

    @Test
    public void testCreateTableWithMapNull() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data map, primary key(id))",
            "insert into {table} values(1, null)"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isNull();
        table.drop();
    }

    @Test
    public void testCreateTableWithMapMixedType() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} (id int, data map, primary key(id))",
            "insert into {table} values(1, map['a', 1, 'b', 2.5])"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isInstanceOf(Map.class)
            .isEqualTo(ImmutableMap.of("a", BigDecimal.valueOf(1), "b", BigDecimal.valueOf(2.5)));
        table.drop();
    }

    @Test
    public void testCreateTableWithMapDefault() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} ("
                + "id int,"
                + "name char(8),"
                + "data map default map['a', 1, 'b', 2],"
                + "primary key(id)"
                + ")",
            "insert into {table}(id, name) values(1, 'ABC')"
        );
        Object result = sqlHelper.querySingleValue("select data from " + table);
        assertThat(result).isInstanceOf(Map.class)
            .isEqualTo(ImmutableMap.of("a", 1, "b", 2));
        table.drop();
    }

    @Test
    public void testCreateTableDateDoubleWithNull() throws SQLException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} ("
                + "id int,"
                + "name varchar(20),"
                + "age int,"
                + "amount double,"
                + "birthday date,"
                + "primary key(id)"
                + ")",
            "insert into {table} values"
                + "(1, 'Steven', 19, 23.5, '2010-01-09'),"
                + "(2, 'Lisi', 18, null, '1987-11-11'),"
                + "(3, 'Kitty', 22, 1000.0, '1990-09-15')"
        );
        Object result = sqlHelper.querySingleValue("select name from " + table + " where id = 3");
        assertThat(result).isEqualTo("Kitty");
        table.drop();
    }

    @Test
    public void testCreateTableWithPartition() throws SQLException, JsonProcessingException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} ("
                + "id int,"
                + "name varchar(20),"
                + "primary key(id)"
                + ") "
                + "partition by range values (2),(3)",
            "insert into {table} values"
                + "(1, 'name1'),"
                + "(2, 'name2'),"
                + "(3, 'name3'),"
                + "(4, 'name4')"
        );
        sqlHelper.queryTest(
            "select * from " + table,
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            "1, name1\n2, name2\n3, name3\n4, name4"
        );
        table.drop();
    }

    @Test
    public void testCreateTableWithPartition1() throws SQLException, JsonProcessingException {
        RandomTable table = sqlHelper.randomTable().execSqls(
            "create table {table} ("
                + "id varchar(20),"
                + "name varchar(20),"
                + "primary key(id, name)"
                + ") "
                + "partition by range values (2),(3)",
            "insert into {table} values"
                + "('11', 'name1'),"
                + "('12', 'name2'),"
                + "('13', 'name3'),"
                + "('14', 'name4')"
        );
        sqlHelper.queryTest(
            "select * from " + table + " where id like '1%'",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("STRING", "STRING"),
            "11, name1\n12, name2\n13, name3\n14, name4"
        );
        table.drop();
    }
}
