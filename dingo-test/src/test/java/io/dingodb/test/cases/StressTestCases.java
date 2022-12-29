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

import io.dingodb.test.RandomTable;
import io.dingodb.test.SqlHelper;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public final class StressTestCases {
    private StressTestCases() {
    }

    public static void testInsert(@NonNull SqlHelper sqlHelper) throws SQLException, IOException {
        RandomTable randomTable = sqlHelper.randomTable();
        randomTable.execFiles("string_double/create.sql");
        Random random = new Random();
        try (Statement statement = sqlHelper.getConnection().createStatement()) {
            for (int id = 1; id <= 1000; ++id) {
                String sql = "insert into {table} values"
                    + "(" + id + ", '" + UUID.randomUUID() + "', " + random.nextDouble() + ")";
                log.info("Exec {}", sql);
                randomTable.execSql(statement, sql);
                assertThat(statement.getUpdateCount()).isEqualTo(1);
            }
        } finally {
            randomTable.drop();
        }
    }

    public static void testInsertWithParameters(@NonNull SqlHelper sqlHelper) throws SQLException, IOException {
        RandomTable randomTable = sqlHelper.randomTable();
        randomTable.execFiles("string_double/create.sql");
        Random random = new Random();
        String sql = "insert into {table} values(?, ?, ?)";
        try (PreparedStatement statement = randomTable.prepare(sql)) {
            for (int id = 1; id <= 1000; ++id) {
                statement.setInt(1, id);
                statement.setString(2, UUID.randomUUID().toString());
                statement.setDouble(3, random.nextDouble());
                log.info("Exec {}, id = {}", sql, id);
                statement.execute();
                assertThat(statement.getUpdateCount()).isEqualTo(1);
            }
        } finally {
            randomTable.drop();
        }
    }

    public static void testInsertWithParametersBatch(@NonNull SqlHelper sqlHelper) throws SQLException, IOException {
        RandomTable randomTable = sqlHelper.randomTable();
        randomTable.execFiles("string_double/create.sql");
        Random random = new Random();
        String sql = "insert into {table} values(?, ?, ?)";
        try (PreparedStatement statement = randomTable.prepare(sql)) {
            for (int id = 1; id <= 1000; ++id) {
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
            randomTable.drop();
        }
    }
}
