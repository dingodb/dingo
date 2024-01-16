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
import io.dingodb.calcite.schema.DingoRootSchema;
import io.dingodb.common.CommonId;
import io.dingodb.meta.MetaService;
import io.dingodb.test.dsl.run.exec.SqlExecContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

import static io.dingodb.test.dsl.builder.SqlTestCaseJavaBuilder.is;

public class ShowTest {
    private static final String tableName = "test";

    private static SqlExecContext context;

    @BeforeAll
    public static void setupAll() throws Exception {
        ConnectionFactory.initLocalEnvironment();
        context = new SqlExecContext(ConnectionFactory.getConnection());
        context.getTableMapping().put("table", tableName);
        context.execSql(
            "create table {table} (\n"
                + "    id int auto_increment,\n"
                + "    name varchar(32),\n"
                + "    age int,\n"
                + "    primary key (id)\n"
                + ") partition by range values (2),(3)"
        );
    }

    @AfterAll
    public static void cleanUpAll() throws SQLException {
        context.cleanUp();
        ConnectionFactory.cleanUp();
    }

    @Test
    public void showCreateTable() throws SQLException {
        String sql = "show create table {table}";
        context.execSql(sql).test(is(
            new String[]{"Table", "Create Table"},
            ImmutableList.of(
                new Object[]{
                    tableName,
                    "create table test (\n"
                        + "    id int auto_increment,\n"
                        + "    name varchar(32),\n"
                        + "    age int,\n"
                        + "    primary key (id)\n"
                        + ") partition by range values (2),(3)"
                }
            )
        ));
    }

    @Test
    public void showAllColumns() throws SQLException {
        String sql = "show columns from {table}";
        context.execSql(sql).test(is(
            new String[]{"Field", "Type", "Null", "Key", "Default"},
            ImmutableList.of(
                new Object[]{"ID", "INTEGER", "NO", "PRI", " "},
                new Object[]{"NAME", "VARCHAR(32)", "YES", " ", "NULL"},
                new Object[]{"AGE", "INTEGER", "YES", " ", "NULL"}
            )
        ));
    }

    @Test
    public void showAllColumnsWithLike() throws SQLException, IOException {
        String sql = "show columns from {table} like 'na%'";
        context.execSql(sql).test(is(
            new String[]{"Field", "Type", "Null", "Key", "Default"},
            ImmutableList.of(
                new Object[]{"NAME", "VARCHAR(32)", "YES", " ", "NULL"}
            )
        ));
    }

    @Test
    public void showTableDistribution() throws SQLException {
        String sql = "show table {table} distribution";
        MetaService metaService = MetaService.root().getSubMetaService(DingoRootSchema.DEFAULT_SCHEMA_NAME);
        CommonId tableId = metaService.getTable(tableName).getTableId();
        context.execSql(sql).test(is(
            new String[]{"Id", "Type", "Value"},
            ImmutableList.of(
                new Object[]{"DISTRIBUTION_" + tableId.seq + "_1", "range", "[ Infinity, Key(2) )"},
                new Object[]{"DISTRIBUTION_" + tableId.seq + "_2", "range", "[ Key(2), Key(3) )"},
                new Object[]{"DISTRIBUTION_" + tableId.seq + "_3", "range", "[ Key(3), Infinity )"}
            )
        ));
    }
}
