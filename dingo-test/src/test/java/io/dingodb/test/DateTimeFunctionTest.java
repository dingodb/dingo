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

import io.dingodb.test.dsl.run.exec.SqlExecContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;

import static io.dingodb.test.dsl.builder.SqlTestCaseJavaBuilder.count;
import static io.dingodb.test.dsl.builder.SqlTestCaseJavaBuilder.is;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DateTimeFunctionTest {
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
    @ValueSource(strings = {
        "now()",
        "current_timestamp",
        "current_timestamp()",
    })
    public void testNow(String fun) throws SQLException {
        String sql = "select " + fun;
        assertThat((Timestamp) context.querySingleValue(sql))
            .isCloseTo(Timestamp.valueOf(LocalDateTime.now()), 3000L);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "curdate()",
        "current_date",
        "current_date()",
    })
    public void testCurDate(String fun) throws SQLException {
        String sql = "select " + fun;
        assertThat(context.querySingleValue(sql).toString())
            .isEqualTo(LocalDate.now().toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "current_time",
        "current_time()",
        "curtime()",
    })
    public void testCurTime(String fun) throws SQLException {
        String sql = "select " + fun;
        assertThat((Time) context.querySingleValue(sql))
            .isCloseTo(Time.valueOf(LocalTime.now()), 3000L);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "current_date",
        "current_date()",
        "curdate()",
        "CURRENT_DATE()",
    })
    public void testDefaultCurrentDate(String fun) throws SQLException {
        String sql = "create table {table} (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    birth date default " + fun + ",\n"
            + "    primary key(id)\n"
            + ")";
        context.execSql(sql);
        sql = "insert into {table}(id, name) values (100, 'lala')";
        context.execSql(sql).test(count(1));
        sql = "select * from {table}";
        context.execSql(sql)
            .test(is(
                new String[]{"id", "name", "birth"},
                Collections.singletonList(new Object[]{100, "lala", Date.valueOf(LocalDate.now())})
            ));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "current_time",
        "current_time()",
        "curtime()",
        "CURRENT_TIME()"
    })
    public void testDefaultCurrentTime(String fun) throws Exception {
        String sql = "create table {table} (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    birth time default " + fun + " ,\n"
            + "    primary key(id)\n"
            + ")";
        context.execSql(sql);
        sql = "insert into {table}(id, name) values (100, 'lala')";
        context.execSql(sql).test(count(1));
        sql = "select * from {table}";
        context.execSql(sql)
            .test(is(
                new String[]{"id", "name", "birth"},
                Collections.singletonList(new Object[]{100, "lala", Time.valueOf(LocalTime.now())})
            ).timeDeviation(5000L));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "now()",
        "current_timestamp",
        "current_timestamp()",
    })
    public void testDefaultCurrentTimestamp(String fun) throws Exception {
        String sql = "create table {table} (\n"
            + "    id int,\n"
            + "    name varchar(32) not null,\n"
            + "    birth timestamp default " + fun + " ,\n"
            + "    primary key(id)\n"
            + ")";
        context.execSql(sql);
        sql = "insert into {table} (id, name) values (100, 'lala')";
        context.execSql(sql).test(count(1));
        sql = "select * from {table}";
        context.execSql(sql)
            .test(is(
                new String[]{"id", "name", "birth"},
                Collections.singletonList(new Object[]{100, "lala", Timestamp.valueOf(LocalDateTime.now())})
            ).timestampDeviation(5000L));
    }
}
