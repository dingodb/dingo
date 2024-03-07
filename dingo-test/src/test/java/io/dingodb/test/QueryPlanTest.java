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
import io.dingodb.test.dsl.run.exec.SqlExecContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.ResultSet;
import java.sql.SQLException;

import static io.dingodb.test.dsl.builder.SqlTestCaseJavaBuilder.file;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class QueryPlanTest {
    private static SqlExecContext context;

    @BeforeAll
    public static void setupAll() throws Exception {
        ConnectionFactory.initLocalEnvironment();
        context = new SqlExecContext(ConnectionFactory.getConnection());
        context.execSql(file("dsl/cases/tables/i4k_vs_f80.create.sql"));
    }

    @AfterAll
    public static void cleanUpAll() throws SQLException {
        context.cleanUp();
        ConnectionFactory.cleanUp();
    }

    @Test
    public void testExplainSimpleValues() throws SQLException {
        String sql = "explain plan for select 1";
        try (ResultSet resultSet = context.execSql(sql).getStatement().getResultSet()) {
            Assert.resultSet(resultSet).isPlan();
        }
    }

    @Test
    public void testExplainInsertValues() throws SQLException {
        String sql = "explain plan for insert into {table} values(1, 'Alice', 1.0)";
        try (ResultSet resultSet = context.execSql(sql).getStatement().getResultSet()) {
            Assert.resultSet(resultSet).isPlan();
        }
    }

    @Test
    public void testExplainScan() throws SQLException {
        String sql = "explain plan for select * from dingo.{table}";
        try (ResultSet resultSet = context.execSql(sql).getStatement().getResultSet()) {
            Assert.resultSet(resultSet).isPlan();
        }
    }
}
