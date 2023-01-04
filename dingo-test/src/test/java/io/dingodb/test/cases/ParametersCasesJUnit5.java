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

package io.dingodb.test.cases;

import com.google.common.collect.ImmutableList;
import io.dingodb.test.RandomTable;
import io.dingodb.test.SqlHelper;
import io.dingodb.test.asserts.Assert;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("MethodMayBeStatic")
public class ParametersCasesJUnit5 implements ArgumentsProvider {
    public void simple(@NonNull SqlHelper sqlHelper) throws SQLException {
        String sql = "select 1 + ?";
        try (PreparedStatement statement = sqlHelper.getConnection().prepareStatement(sql)) {
            statement.setInt(1, 1);
            try (ResultSet resultSet = statement.executeQuery()) {
                Assert.resultSet(resultSet).isRecords(ImmutableList.of(
                    new Object[]{2}
                ));
            }
            statement.setInt(1, 2);
            try (ResultSet resultSet = statement.executeQuery()) {
                Assert.resultSet(resultSet).isRecords(ImmutableList.of(
                    new Object[]{3}
                ));
            }
        }
    }

    public void query(@NonNull SqlHelper sqlHelper) throws SQLException, IOException {
        RandomTable randomTable = sqlHelper.randomTable();
        randomTable.execFiles(
            "string_double/create.sql",
            "string_double/data.sql"
        );
        String sql = "select * from {table} where id < ? and amount > ?";
        try (PreparedStatement statement = randomTable.prepare(sql)) {
            statement.setInt(1, 8);
            statement.setDouble(2, 5.0);
            try (ResultSet resultSet = statement.executeQuery()) {
                Assert.resultSet(resultSet).isRecords(ImmutableList.of(
                    new Object[]{5, "Emily", 5.5},
                    new Object[]{6, "Alice", 6.0},
                    new Object[]{7, "Betty", 6.5}
                ));
            }
            statement.setDouble(2, 6.0);
            try (ResultSet resultSet = statement.executeQuery()) {
                Assert.resultSet(resultSet).isRecords(ImmutableList.of(
                    new Object[]{7, "Betty", 6.5}
                ));
            }
        }
        randomTable.drop();
    }

    public void filter(@NonNull SqlHelper sqlHelper) throws SQLException, IOException {
        RandomTable randomTable = sqlHelper.randomTable();
        randomTable.execFiles(
            "string_double/create.sql",
            "string_double/data.sql"
        );
        String sql = "select * from {table} where id < ? and amount > ?";
        try (PreparedStatement statement = randomTable.prepare(sql)) {
            statement.setInt(1, 8);
            statement.setDouble(2, 5.0);
            try (ResultSet resultSet = statement.executeQuery()) {
                Assert.resultSet(resultSet).isRecords(ImmutableList.of(
                    new Object[]{5, "Emily", 5.5},
                    new Object[]{6, "Alice", 6.0},
                    new Object[]{7, "Betty", 6.5}
                ));
            }
            statement.setDouble(2, 6.0);
            try (ResultSet resultSet = statement.executeQuery()) {
                Assert.resultSet(resultSet).isRecords(ImmutableList.of(
                    new Object[]{7, "Betty", 6.5}
                ));
            }
        } finally {
            randomTable.drop();
        }
    }

    public void filterWithFun(@NonNull SqlHelper sqlHelper) throws SQLException, IOException {
        RandomTable randomTable = sqlHelper.randomTable();
        randomTable.execFiles(
            "string_double/create.sql",
            "string_double/data.sql"
        );
        String sql = "select * from {table} where locate(?, name) <> 0";
        try (PreparedStatement statement = randomTable.prepare(sql)) {
            statement.setString(1, "i");
            try (ResultSet resultSet = statement.executeQuery()) {
                Assert.resultSet(resultSet).isRecords(ImmutableList.of(
                    new Object[]{1, "Alice", 3.5},
                    new Object[]{3, "Cindy", 4.5},
                    new Object[]{4, "Doris", 5.0},
                    new Object[]{5, "Emily", 5.5},
                    new Object[]{6, "Alice", 6.0},
                    new Object[]{8, "Alice", 7.0},
                    new Object[]{9, "Cindy", 7.5}
                ));
            }
            statement.setString(1, "o");
            try (ResultSet resultSet = statement.executeQuery()) {
                Assert.resultSet(resultSet).isRecords(ImmutableList.of(
                    new Object[]{4, "Doris", 5.0}
                ));
            }
        } finally {
            randomTable.drop();
        }
    }

    public void insert(@NonNull SqlHelper sqlHelper) throws SQLException, IOException {
        RandomTable randomTable = sqlHelper.randomTable();
        randomTable.execFiles(
            "string_double/create.sql",
            "string_double/data.sql"
        );
        String sql = "insert into {table} values(?, ?, ?)";
        try (PreparedStatement statement = randomTable.prepare(sql)) {
            statement.setInt(1, 10);
            statement.setString(2, "Alice");
            statement.setDouble(3, 10.0);
            int count = statement.executeUpdate();
            assertThat(count).isEqualTo(1);
            statement.setInt(1, 11);
            statement.setString(2, "Betty");
            statement.setDouble(3, 11.0);
            count = statement.executeUpdate();
            assertThat(count).isEqualTo(1);
        }
        randomTable.queryTest(
            "select * from {table} where id >= 10",
            new String[]{"id", "name", "amount"},
            ImmutableList.of(
                new Object[]{10, "Alice", 10.0},
                new Object[]{11, "Betty", 11.0}
            )
        );
        randomTable.drop();
    }

    public void insertWithDateTime(@NonNull SqlHelper sqlHelper) throws SQLException {
        RandomTable randomTable = sqlHelper.randomTable();
        randomTable.execSqls(
            "create table {table} ("
                + "id int,"
                + "data date,"
                + "data1 time,"
                + "data2 timestamp,"
                + "primary key(id)"
                + ")"
        );
        String sql = "insert into {table} values(?, ?, ?, ?)";
        try (PreparedStatement statement = randomTable.prepare(sql)) {
            statement.setInt(1, 1);
            statement.setDate(2, new Date(0));
            statement.setTime(3, new Time(0));
            statement.setTimestamp(4, new Timestamp(0));
            int count = statement.executeUpdate();
            assertThat(count).isEqualTo(1);
            statement.setInt(1, 2);
            statement.setDate(2, new Date(86400000));
            statement.setTime(3, new Time(3600000));
            statement.setTimestamp(4, new Timestamp(1));
            count = statement.executeUpdate();
            assertThat(count).isEqualTo(1);
            randomTable.queryTest(
                "select * from " + randomTable,
                new String[]{"id", "data", "data1", "data2"},
                ImmutableList.of(
                    new Object[]{1, new Date(0).toString(), new Time(0).toString(), new Timestamp(0)},
                    new Object[]{2, new Date(86400000).toString(), new Time(3600000).toString(), new Timestamp(1)}
                )
            );
        } finally {
            randomTable.drop();
        }
    }

    public void insertBatch(@NonNull SqlHelper sqlHelper) throws SQLException, IOException {
        RandomTable randomTable = sqlHelper.randomTable();
        randomTable.execFiles(
            "string_double/create.sql",
            "string_double/data.sql"
        );
        String sql = "insert into {table} values(?, ?, ?)";
        try (PreparedStatement statement = randomTable.prepare(sql)) {
            statement.setInt(1, 12);
            statement.setString(2, "Alice");
            statement.setDouble(3, 12.0);
            statement.addBatch();
            statement.setInt(1, 13);
            statement.setString(2, "Betty");
            statement.setDouble(3, 13.0);
            statement.addBatch();
            int[] count = statement.executeBatch();
            assertThat(count).isEqualTo(new int[]{1, 1});
            randomTable.queryTest(
                "select * from {table} where id >= 10",
                new String[]{"id", "name", "amount"},
                ImmutableList.of(
                    new Object[]{12, "Alice", 12.0},
                    new Object[]{13, "Betty", 13.0}
                )
            );
        } finally {
            randomTable.drop();
        }
    }

    @Override
    public Stream<? extends Arguments> provideArguments(@NonNull ExtensionContext context) {
        return Stream.of(
            ClassTestMethod.argumentsOf(this::simple, "Simple values"),
            ClassTestMethod.argumentsOf(this::query, "Query"),
            ClassTestMethod.argumentsOf(this::filter, "Filter"),
            ClassTestMethod.argumentsOf(this::filterWithFun, "Filter with fun"),
            ClassTestMethod.argumentsOf(this::insert, "Insert"),
            ClassTestMethod.argumentsOf(this::insertBatch, "Insert batch"),
            ClassTestMethod.argumentsOf(this::insertWithDateTime, "Insert with date/time")
        );
    }
}
