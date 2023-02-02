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

package io.dingodb.test.cases.provider;

import io.dingodb.test.cases.Case;
import io.dingodb.test.cases.ClassTestMethod;
import io.dingodb.test.cases.RandomTable;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static io.dingodb.test.cases.Case.exec;
import static io.dingodb.test.cases.Case.file;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("MethodMayBeStatic")
@Slf4j
public final class StressCasesJUnit5 implements ArgumentsProvider {
    private static final int RECORD_NUM = 100;

    public void insert(@NonNull Connection connection) throws Exception {
        RandomTable randomTable = new RandomTable();
        Case.of(
            exec(file("string_double/create.sql"))
        ).run(connection, randomTable);
        Random random = new Random();
        try (Statement statement = connection.createStatement()) {
            for (int id = 1; id <= RECORD_NUM; ++id) {
                String sql = "insert into {table} values"
                    + "(" + id + ", '" + UUID.randomUUID() + "', " + random.nextDouble() + ")";
                log.info("Exec {}", sql);
                statement.execute(randomTable.transSql(sql));
                assertThat(statement.getUpdateCount()).isEqualTo(1);
            }
        } finally {
            Case.dropRandomTables(connection, randomTable);
        }
    }

    public void insertWithParameters(@NonNull Connection connection) throws Exception {
        RandomTable randomTable = new RandomTable();
        Case.of(
            exec(file("string_double/create.sql"))
        ).run(connection, randomTable);
        Random random = new Random();
        String sql = "insert into {table} values(?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(randomTable.transSql(sql))) {
            for (int id = 1; id <= RECORD_NUM; ++id) {
                statement.setInt(1, id);
                statement.setString(2, UUID.randomUUID().toString());
                statement.setDouble(3, random.nextDouble());
                log.info("Exec {}, id = {}", sql, id);
                statement.execute();
                assertThat(statement.getUpdateCount()).isEqualTo(1);
            }
        } finally {
            Case.dropRandomTables(connection, randomTable);
        }
    }

    public void insertWithParametersBatch(@NonNull Connection connection) throws Exception {
        RandomTable randomTable = new RandomTable();
        Case.of(
            exec(file("string_double/create.sql"))
        ).run(connection, randomTable);
        Random random = new Random();
        String sql = "insert into {table} values(?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(randomTable.transSql(sql))) {
            for (int id = 1; id <= RECORD_NUM; ++id) {
                statement.setInt(1, id);
                statement.setString(2, UUID.randomUUID().toString());
                statement.setDouble(3, random.nextDouble());
                statement.addBatch();
                if (id % 100 == 0) {
                    int[] updateCounts = statement.executeBatch();
                    log.info("execute batch {}, id = {}", sql, id);
                    assertThat(updateCounts).containsOnly(1);
                    statement.clearBatch();
                }
            }
        } finally {
            Case.dropRandomTables(connection, randomTable);
        }
    }

    @Override
    public @NonNull Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        return Stream.of(
            ClassTestMethod.argumentsOf(this::insert, "Insert"),
            ClassTestMethod.argumentsOf(this::insertWithParameters, "Insert with parameters"),
            ClassTestMethod.argumentsOf(this::insertWithParametersBatch, "Insert with parameters batch")
        );
    }
}
