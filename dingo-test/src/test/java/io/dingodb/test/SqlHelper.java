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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.common.type.DingoType;
import io.dingodb.meta.local.LocalMetaService;
import io.dingodb.test.asserts.Assert;
import io.dingodb.test.utils.CsvUtils;
import io.dingodb.test.utils.ResultSetUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Deprecated
@Slf4j
public class SqlHelper {
    @Getter
    private final Connection connection;

    public SqlHelper() throws Exception {
        ConnectionFactory.initLocalEnvironment();
        connection = ConnectionFactory.getConnection();
    }

    public SqlHelper(Connection connection) {
        this.connection = connection;
    }

    @Nonnull
    public static String randomTableName() {
        return "tbl_" + UUID.randomUUID().toString().replace('-', '_');
    }

    public static void execSql(Statement statement, @NonNull String sql) throws SQLException {
        for (String s : sql.split(";")) {
            if (!s.trim().isEmpty()) {
                statement.execute(s);
            }
        }
    }

    public void exceptionTest(
        List<String> sqlList,
        boolean needDropping,
        int sqlCode,
        String sqlState
    ) {
        RandomTable randomTable = randomTable();
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
        assertThat(exception.getErrorCode()).isEqualTo(sqlCode);
        assertThat(exception.getSQLState()).isEqualTo(sqlState);
    }

    public void execSql(@Nonnull String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            execSql(statement, sql);
        }
    }

    public void cleanUp() throws SQLException {
        connection.close();
        LocalMetaService.clear();
    }

    public RandomTable randomTable() {
        return new RandomTable(this);
    }

    public DatabaseMetaData metaData() throws SQLException {
        return connection.getMetaData();
    }

    public ResultSetUtils.Row querySingleRow(String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                int count = 0;
                ResultSetUtils.Row row = null;
                while (resultSet.next()) {
                    ++count;
                    row = ResultSetUtils.getRow(resultSet);
                }
                assertThat(count).isEqualTo(1);
                return row;
            }
        }
    }

    public Object querySingleValue(String sql) throws SQLException {
        ResultSetUtils.Row row = querySingleRow(sql);
        Object[] tuple = row.getTuple();
        assertThat(tuple).hasSize(1);
        return tuple[0];
    }

    public void queryTest(
        String sql,
        String[] columns,
        List<Object[]> tuples
    ) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                Assert.resultSet(resultSet)
                    .columnLabels(columns)
                    .isRecords(tuples);
            }
        }
    }

    public void queryTest(
        String sql,
        String[] columns,
        DingoType schema,
        String data
    ) throws SQLException, JsonProcessingException {
        queryTest(sql, columns, CsvUtils.readCsv(schema, data));
    }

    public void queryTestInOrder(
        String sql,
        String[] columns,
        List<Object[]> data
    ) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                Assert.resultSet(resultSet)
                    .columnLabels(columns)
                    .isRecordsInOrder(data);
            }
        }
    }

    public void queryTestInOrder(
        String sql,
        String[] columns,
        DingoType schema,
        String data
    ) throws SQLException, JsonProcessingException {
        queryTestInOrder(sql, columns, CsvUtils.readCsv(schema, data));
    }

    // compare time in hours
    public void queryTestInOrderWithApproxTime(
        String sql,
        String[] columns,
        List<Object[]> data
    ) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                Assert.resultSet(resultSet)
                    .columnLabels(columns)
                    .isRecordsInOrderWithApproxTime(data);
            }
        }
    }

    public void explainTest(String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                Assert.resultSet(resultSet).isPlan();
            }
        }
    }

    public void updateTest(String sql, int affectedRows) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            int count = statement.executeUpdate(sql);
            assertThat(count).isEqualTo(affectedRows);
        }
    }

    public void execFile(@Nonnull InputStream stream) throws IOException, SQLException {
        String sql = IOUtils.toString(stream, StandardCharsets.UTF_8);
        try (Statement statement = connection.createStatement()) {
            execSql(statement, sql);
        }
    }

    public void execFile(@Nonnull String sqlFile) throws IOException, SQLException {
        execFile(Objects.requireNonNull(SqlHelper.class.getResourceAsStream(sqlFile)));
    }

    public void clearTable(String tableName) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from " + tableName);
        }
    }

    public void dropTable(String tableName) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop table " + tableName);
        }
    }
}
