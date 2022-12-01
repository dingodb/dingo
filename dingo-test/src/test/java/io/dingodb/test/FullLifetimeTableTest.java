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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

public class FullLifetimeTableTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @Test
    public void testInsertDoubleAndNull() throws SQLException {
        sqlHelper.doTest(
            "create table {table} (id int not null, data double, primary key(id))",
            "insert into {table} values (1, 2), (2, null)",
            2,
            "select * from {table}",
            new String[]{"id", "data"},
            Arrays.asList(
                new Object[]{1, 2.0},
                new Object[]{2, null}
            )
        );
    }

    @Test
    public void testQueryBoolean() throws SQLException {
        sqlHelper.doTest(
            "create table {table} (id int not null, data boolean, primary key(id))",
            "insert into {table} values (1, 0), (2, 1)",
            2,
            "select * from {table} where data",
            new String[]{"id", "data"},
            Collections.singletonList(new Object[]{2, true})
        );
    }

    @Test
    public void testDatePrimaryKey() throws SQLException {
        sqlHelper.doTest(
            "create table {table} (id date not null, data int, primary key(id))",
            "insert into {table} values ('1980-1-1', 0), ('1990-2-2', 1)",
            2,
            "select data from {table}",
            new String[]{"data"},
            Arrays.asList(
                new Object[]{0},
                new Object[]{1}
            )
        );
    }

    @Test
    public void testStringFunNull() throws SQLException {
        sqlHelper.doTest(
            "create table {table} (id int not null, s1 varchar(20), s2 varchar(20), primary key(id))",
            "insert into {table} values (1, null, 'abc')",
            1,
            "select concat(s1, s2) as res from {table}",
            new String[]{"res"},
            Collections.singletonList(
                new Object[]{null}
            )
        );
    }

    @Test
    public void testPowFun() throws SQLException {
        sqlHelper.doTest(
            "create table {table} (id int, name varchar(20), age int, amount double, primary key(id))",
            "insert into {table} values"
                + "(1, 'Alice', 10, 2.58),"
                + "(2, 'Betty', 25, 109.11)",
            2,
            "select pow(age, id) pai from {table}",
            new String[]{"pai"},
            ImmutableList.of(
                new Object[]{BigDecimal.valueOf(10)},
                new Object[]{BigDecimal.valueOf(625)}
            )
        );
    }

    @Test
    public void testModFun() throws SQLException {
        sqlHelper.doTest(
            "create table {table} (id int, name varchar(20), age int, amount double, primary key(id))",
            "insert into {table} values"
                + "(1, 'Alice', 10, 2.58),"
                + "(2, 'Betty', 25, 109.11)",
            2,
            "select mod(amount, age) from {table}",
            new String[]{"EXPR$0"},
            ImmutableList.of(
                new Object[]{BigDecimal.valueOf(2.58)},
                new Object[]{BigDecimal.valueOf(9.11)}
            )
        );
    }
}
