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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.driver.DingoDriver;
import io.dingodb.exec.Services;
import io.dingodb.meta.Part;
import io.dingodb.meta.local.MetaService;
import io.dingodb.test.asserts.Assert;
import io.dingodb.test.asserts.AssertResultSet;
import io.dingodb.test.util.CsvUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class SqlHelper {
    private static final String TABLE_NAME_PLACEHOLDER = "{table}";

    @Getter
    private final Connection connection;

    public SqlHelper() throws Exception {
        // Configure for local test.
        if (DingoConfiguration.instance() == null) {
            DingoConfiguration.parse(
                Objects.requireNonNull(SqlHelper.class.getResource("/config.yaml")).getPath()
            );
        }
        Services.initNetService();
        Services.NET.listenPort(FakeLocation.PORT);
        connection = getLocalConnection();
        TreeMap<ByteArrayUtils.ComparableByteArray, Part> defaultPart = new TreeMap<>();
        byte[] startKey = ByteArrayUtils.EMPTY_BYTES;
        byte[] endKey = ByteArrayUtils.MAX_BYTES;
        defaultPart.put(new ByteArrayUtils.ComparableByteArray(startKey), new Part(
            startKey,
            new FakeLocation(1),
            Collections.singletonList(new FakeLocation(1)),
            startKey,
            endKey
        ));
        MetaService metaService = MetaService.ROOT;
        metaService.setParts(defaultPart);
        MetaService.setLocation(new FakeLocation(0));
    }

    public SqlHelper(Connection connection) {
        this.connection = connection;
    }

    public static Connection getLocalConnection() throws ClassNotFoundException, SQLException, IOException {
        Class.forName("io.dingodb.driver.DingoDriver");
        Properties properties = new Properties();
        properties.load(SqlHelper.class.getResourceAsStream("/test.properties"));
        return DriverManager.getConnection(
            DingoDriver.CONNECT_STRING_PREFIX,
            properties
        );
    }

    @Nonnull
    public static String randomTableName() {
        return "tbl_" + UUID.randomUUID().toString().replace('-', '_');
    }

    public String prepareTable(
        @Nonnull String createSql
    ) throws SQLException {
        String tableName = randomTableName();
        execSqlCmd(createSql.replace(TABLE_NAME_PLACEHOLDER, tableName));
        return tableName;
    }

    public String prepareTable(
        @Nonnull String createSql,
        @Nonnull String insertSql
    ) throws SQLException {
        String tableName = prepareTable(createSql);
        execSqlCmd(insertSql.replace(TABLE_NAME_PLACEHOLDER, tableName));
        return tableName;
    }

    public String prepareTable(
        @Nonnull String createSql,
        @Nonnull String insertSql,
        int affectedRows
    ) throws SQLException {
        String tableName = prepareTable(createSql);
        updateTest(insertSql.replace(TABLE_NAME_PLACEHOLDER, tableName), affectedRows);
        return tableName;
    }

    public void doTest(
        @Nonnull String createSql,
        @Nonnull String insertSql,
        int affectedRows,
        @Nonnull String querySql,
        String[] columnLabels,
        List<Object[]> results
    ) throws SQLException {
        String tableName = prepareTable(createSql, insertSql, affectedRows);
        queryTest(querySql.replace(TABLE_NAME_PLACEHOLDER, tableName), columnLabels, results);
        dropTable(tableName);
    }

    public void cleanUp() throws SQLException {
        connection.close();
        MetaService.clear();
        MetaTestService.INSTANCE.clear();
    }

    public DatabaseMetaData metaData() throws SQLException {
        return connection.getMetaData();
    }

    public Object[] querySingleRow(String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                int count = 0;
                Object[] row = null;
                while (resultSet.next()) {
                    ++count;
                    row = AssertResultSet.getRow(resultSet);
                }
                assertThat(count).isEqualTo(1);
                return row;
            }
        }
    }

    public Object querySingleValue(String sql) throws SQLException {
        Object[] row = querySingleRow(sql);
        assertThat(row).hasSize(1);
        return row[0];
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

    public void queryTest(String sql, InputStream resultFile) throws IOException, SQLException {
        Iterator<String[]> it = CsvUtils.readCsv(resultFile);
        final String[] columnNames = it.hasNext() ? it.next() : null;
        final DingoType schema = it.hasNext() ? DingoTypeFactory.tuple(it.next()) : null;
        if (columnNames == null || schema == null) {
            throw new IllegalArgumentException(
                "Result file must be csv and its first two rows are column names and schema definitions."
            );
        }
        List<Object[]> tuples = ImmutableList.copyOf(Iterators.transform(it, i -> (Object[]) schema.parse(i)));
        queryTest(sql, columnNames, tuples);
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

    public void queryTestInOrderWithApproxTime(
        String sql,
        String[] columns,
        DingoType schema,
        String data
    ) throws SQLException, JsonProcessingException {
        queryTestInOrderWithApproxTime(sql, columns, CsvUtils.readCsv(schema, data));
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
    public int execSqlCmd(@Nonnull String sqlCmd) throws SQLException {
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

    public void dropTable(String tableName) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop table " + tableName);
        }
    }

    public void doQueryTest(
        @Nonnull Class<?> testClass,
        String sqlFileName,
        String resultFileName
    ) throws SQLException, IOException {
        String sql = IOUtils.toString(
            Objects.requireNonNull(testClass.getResourceAsStream(sqlFileName + ".sql")),
            StandardCharsets.UTF_8
        );
        queryTest(sql, testClass.getResourceAsStream(resultFileName + ".csv"));
    }

    public void doQueryTest(Class<?> testClass, String fileName) throws SQLException, IOException {
        doQueryTest(testClass, fileName, fileName);
    }
}
