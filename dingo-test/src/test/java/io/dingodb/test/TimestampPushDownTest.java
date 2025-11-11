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
import io.dingodb.test.dsl.run.exec.SqlExecContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;

import static io.dingodb.test.dsl.builder.SqlTestCaseJavaBuilder.is;

public class TimestampPushDownTest {
    private static final String tableName = "test";

    private static SqlExecContext context;

    @BeforeAll
    public static void setupAll() throws Exception {
        ConnectionFactory.initLocalEnvironment();
        context = new SqlExecContext(ConnectionFactory.getConnection());
        context.getTableMapping().put("table", tableName);
        context.execSql(
            "create table {table} (\n"
                + "    id int,\n"
                + "    a timestamp,\n"
                + "    b timestamp,\n"
                + "    primary key (id)\n"
                + ")"
        );
        context.execSql(
            "insert into {table} values(\n"
                + "    1,\n"
                + "    '2020-02-01 12:12:12',\n"
                + "    '2020-02-01 12:12:12'\n"
                + ")"
        );
        context.execSql(
            "insert into {table} values(\n"
                + "    2,\n"
                + "    '2020-02-01 12:12:12',\n"
                + "    '2020-02-02 12:12:12'\n"
                + ")"
        );
        context.execSql(
            "insert into {table} values(\n"
                + "    3,\n"
                + "    '2020-02-03 12:12:12',\n"
                + "    '2020-02-02 12:12:12'\n"
                + ")"
        );
        context.execSql(
            "insert into {table} values(\n"
                + "    4,\n"
                + "    null,\n"
                + "    null\n"
                + ")"
        );
    }

    @AfterAll
    public static void cleanUpAll() throws SQLException {
        context.cleanUp();
        ConnectionFactory.cleanUp();
    }

    @Test
    public void testFilterEQ() throws SQLException {
        String sql = "select * from {table} where a = b";
        context.execSql(sql).test(is(
            new String[]{"id", "a", "b"},
            ImmutableList.of(
                new Object[]{1, Timestamp.valueOf("2020-02-01 12:12:12"), Timestamp.valueOf("2020-02-01 12:12:12.0")}
            )
        ));
    }

    @Test
    public void testFilterNE() throws SQLException {
        String sql = "select * from {table} where a != b";
        context.execSql(sql).test(is(
            new String[]{"id", "a", "b"},
            ImmutableList.of(
                new Object[]{2, Timestamp.valueOf("2020-02-01 12:12:12"), Timestamp.valueOf("2020-02-02 12:12:12.0")},
                new Object[]{3, Timestamp.valueOf("2020-02-03 12:12:12"), Timestamp.valueOf("2020-02-02 12:12:12.0")}
            )
        ));
    }

    @Test
    public void testFilterGT() throws SQLException {
        String sql = "select * from {table} where a > b";
        context.execSql(sql).test(is(
            new String[]{"id", "a", "b"},
            ImmutableList.of(
                new Object[]{3, Timestamp.valueOf("2020-02-03 12:12:12"), Timestamp.valueOf("2020-02-02 12:12:12.0")}
            )
        ));
    }

    @Test
    public void testFilterGE() throws SQLException {
        String sql = "select * from {table} where a >= b";
        context.execSql(sql).test(is(
            new String[]{"id", "a", "b"},
            ImmutableList.of(
                new Object[]{1, Timestamp.valueOf("2020-02-01 12:12:12"), Timestamp.valueOf("2020-02-01 12:12:12.0")},
                new Object[]{3, Timestamp.valueOf("2020-02-03 12:12:12"), Timestamp.valueOf("2020-02-02 12:12:12.0")}
            )
        ));
    }

    @Test
    public void testFilterLT() throws SQLException {
        String sql = "select * from {table} where a < b";
        context.execSql(sql).test(is(
            new String[]{"id", "a", "b"},
            ImmutableList.of(
                new Object[]{2, Timestamp.valueOf("2020-02-01 12:12:12"), Timestamp.valueOf("2020-02-02 12:12:12.0")}
            )
        ));
    }

    @Test
    public void testFilterLE() throws SQLException {
        String sql = "select * from {table} where a <= b";
        context.execSql(sql).test(is(
            new String[]{"id", "a", "b"},
            ImmutableList.of(
                new Object[]{1, Timestamp.valueOf("2020-02-01 12:12:12"), Timestamp.valueOf("2020-02-01 12:12:12.0")},
                new Object[]{2, Timestamp.valueOf("2020-02-01 12:12:12"), Timestamp.valueOf("2020-02-02 12:12:12.0")}
            )
        ));
    }

    @Test
    public void testFilterEqNull() throws SQLException {
        String sql = "select * from {table} where a = null";
        context.execSql(sql).test(is(
            new String[]{"id", "a", "b"},
            ImmutableList.of(
            )
        ));
    }

    @Test
    public void testFilterIsNull() throws SQLException {
        String sql = "select * from {table} where a is null";
        context.execSql(sql).test(is(
            new String[]{"id", "a", "b"},
            ImmutableList.of(
                new Object[]{4, null, null}
            )
        ));
    }

    @Test
    public void testFilterIsNotNull() throws SQLException {
        String sql = "select * from {table} where a is not null";
        context.execSql(sql).test(is(
            new String[]{"id", "a", "b"},
            ImmutableList.of(
                new Object[]{1, Timestamp.valueOf("2020-02-01 12:12:12"), Timestamp.valueOf("2020-02-01 12:12:12.0")},
                new Object[]{2, Timestamp.valueOf("2020-02-01 12:12:12"), Timestamp.valueOf("2020-02-02 12:12:12.0")},
                new Object[]{3, Timestamp.valueOf("2020-02-03 12:12:12"), Timestamp.valueOf("2020-02-02 12:12:12.0")}
            )
        ));
    }

    @Test
    public void testProjectMin() throws SQLException {
        String sql = "select min(a) from {table}";
        context.execSql(sql).test(is(
            new String[]{"min(a)"},
            ImmutableList.of(
                new Object[]{Timestamp.valueOf("2020-02-01 12:12:12")}
            )
        ));
    }

    @Test
    public void testProjectMax() throws SQLException {
        String sql = "select max(a) from {table}";
        context.execSql(sql).test(is(
            new String[]{"max(a)"},
            ImmutableList.of(
                new Object[]{Timestamp.valueOf("2020-02-03 12:12:12")}
            )
        ));
    }

    @Test
    public void testProjectCount() throws SQLException {
        String sql = "select count(a) from {table}";
        context.execSql(sql).test(is(
            new String[]{"count(a)"},
            ImmutableList.of(
                new Object[]{3L}
            )
        ));
    }

    @Test
    public void testProjectCountStar() throws SQLException {
        String sql = "select count(*) from {table}";
        context.execSql(sql).test(is(
            new String[]{"count(*)"},
            ImmutableList.of(
                new Object[]{4L}
            )
        ));
    }

}
