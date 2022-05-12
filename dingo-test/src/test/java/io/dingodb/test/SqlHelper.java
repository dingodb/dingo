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
import com.google.common.collect.Iterators;
import io.dingodb.calcite.Connections;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.common.util.CsvUtils;
import io.dingodb.common.util.StackTraces;
import io.dingodb.exec.Services;
import io.dingodb.meta.test.MetaTestService;
import io.dingodb.test.asserts.AssertResultSet;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class SqlHelper {
    @Getter
    private final Connection connection;

    public SqlHelper() throws Exception {
        if (DingoConfiguration.instance() == null) {
            System.out.println(StackTraces.lineNumber());
            DingoConfiguration.parse(SqlHelper.class.getResource("/config.yaml").getPath());
            System.out.println(StackTraces.lineNumber());
        }
        Services.metaServices.get(MetaTestService.SCHEMA_NAME).init(null);
        Services.initNetService();
        connection = Connections.getConnection(MetaTestService.SCHEMA_NAME);
    }

    public SqlHelper(Connection connection) {
        this.connection = connection;
    }

    public void cleanUp() throws SQLException {
        connection.close();
        Services.metaServices.get(MetaTestService.SCHEMA_NAME).clear();
    }

    public DatabaseMetaData metaData() throws SQLException {
        return connection.getMetaData();
    }

    public void queryTest(
        String sql,
        String[] columns,
        List<Object[]> tuples
    ) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet)
                    .columnLabels(columns)
                    .isRecords(tuples);
            }
        }
    }

    public void queryTest(
        String sql,
        String[] columns,
        TupleSchema schema,
        String data
    ) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet)
                    .columnLabels(columns)
                    .isRecords(schema, data);
            }
        }
    }

    public void queryTest(String sql, InputStream resultFile) throws IOException, SQLException {
        Iterator<String[]> it = CsvUtils.readCsv(resultFile);
        String[] columnNames = null;
        if (it.hasNext()) {
            columnNames = it.next();
        }
        TupleSchema schema = null;
        if (it.hasNext()) {
            schema = TupleSchema.ofTypes(it.next());
        }
        if (columnNames == null || schema == null) {
            throw new IllegalArgumentException("Result file must be csv, "
                + "and its first two rows are column names and schema definitions.");
        }
        List<Object[]> tuples = ImmutableList.copyOf(Iterators.transform(it, schema::parse));
        queryTest(sql, columnNames, tuples);
    }

    public void queryTestOrder(
        String sql,
        String[] columns,
        TupleSchema schema,
        String data
    ) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet)
                    .columnLabels(columns)
                    .isRecordsInOrder(schema, data);
            }
        }
    }


    // compare time in hours
    public void queryTestWithTime(
        String sql,
        String[] columns,
        TupleSchema schema,
        List<Object[]> data
    ) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet)
                    .columnLabels(columns)
                    .isRecordsWithTime(data);
            }
        }
    }

    public void explainTest(String sql, String... data) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isPlan(data);
            }
        }
    }

    public void updateTest(String sql, int affectedRows) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            int count = statement.executeUpdate(sql);
            assertThat(count).isEqualTo(affectedRows);
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    public int execFile(@Nullable InputStream stream) throws IOException, SQLException {
        Objects.requireNonNull(stream);
        int result = -1;
        String[] sqlList = IOUtils.toString(stream, StandardCharsets.UTF_8).split(";");
        try (Statement statement = connection.createStatement()) {
            for (String sql : sqlList) {
                if (!sql.trim().isEmpty()) {
                    result = statement.executeUpdate(sql);
                }
            }
        }
        return result;
    }

    @SuppressWarnings("UnusedReturnValue")
    public int execFile(@Nonnull String sqlFile) throws IOException, SQLException {
        return execFile(SqlHelper.class.getResourceAsStream(sqlFile));
    }

    @SuppressWarnings("UnusedReturnValue")
    public int execSqlCmd(@Nonnull String sqlCmd) throws IOException, SQLException {
        int result = -1;
        try (Statement statement = connection.createStatement()) {
            if (!sqlCmd.trim().isEmpty()) {
                result = statement.executeUpdate(sqlCmd);
            }
        }
        return result;
    }

    public void clearTable(String tableName) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from " + tableName);
        }
    }
}
